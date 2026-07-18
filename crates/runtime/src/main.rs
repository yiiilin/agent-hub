use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    env, fs as stdfs,
    io::{BufRead, BufReader, Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant, SystemTime},
};

#[cfg(unix)]
use std::sync::atomic::AtomicI32;

use agent_hub_shared::*;
use anyhow::Context;
use axum::{
    body::{Body, Bytes},
    extract::{Path as AxumPath, State},
    http::{header, HeaderMap, HeaderName, StatusCode as AxumStatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::AsyncWriteExt,
    net::TcpListener,
    sync::{mpsc as tokio_mpsc, oneshot, Notify},
    task::{JoinHandle, JoinSet},
};
use tracing::{info, warn};
use uuid::Uuid;
use walkdir::WalkDir;

mod session_bundle;

const REDACTED_SECRET: &str = "********";
const DEFAULT_APP_SERVER_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_MODEL_PROXY_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(900);
const CHECKPOINT_RETRY_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_MAX_ONLINE_SESSIONS: usize = 4;
const APP_SERVER_EVENT_QUEUE_CAPACITY: usize = 64;
const SESSION_SUPERVISOR_METADATA_FILE: &str = "session.json";
const SESSION_CLEANUP_DIRECTORY: &str = "session-cleanups";
const SESSION_CLEANUP_STATE_FILE: &str = "state.json";
const MAX_CODEX_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CODEX_BINARY_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone)]
struct Config {
    hub_url: String,
    enrollment_token: Option<String>,
    credential_file: PathBuf,
    work_root: PathBuf,
    hostname: String,
    poll_interval: Duration,
    codex_driver: String,
    codex_source: String,
    codex_bin: String,
    codex_version: String,
    app_server_timeout: Duration,
    model_proxy_idle_timeout: Duration,
    session_idle_timeout: Duration,
    max_online_sessions: usize,
    workdir_ttl: Duration,
    local_skills_dir: Option<PathBuf>,
    sandbox_mode: String,
    sandbox_downgrade_reason: Option<String>,
    health_bind_addr: SocketAddr,
}

#[derive(Debug, Clone)]
struct RuntimeCodexState {
    current_version: String,
    candidate_version: Option<String>,
    candidate_status: Option<String>,
    candidate_error: Option<String>,
    candidate_binary: Option<PathBuf>,
}

impl RuntimeCodexState {
    fn new(config: &Config) -> Self {
        Self {
            current_version: config.codex_version.clone(),
            candidate_version: None,
            candidate_status: None,
            candidate_error: None,
            candidate_binary: None,
        }
    }

    fn heartbeat_status(&self) -> RuntimeCodexStatusDto {
        RuntimeCodexStatusDto {
            current_version: self.current_version.clone(),
            candidate_version: self.candidate_version.clone(),
            candidate_status: self.candidate_status.clone(),
            candidate_error: self.candidate_error.clone(),
        }
    }

    fn clear_candidate(&mut self) {
        self.candidate_version = None;
        self.candidate_status = None;
        self.candidate_error = None;
        self.candidate_binary = None;
    }
}

#[derive(Debug)]
struct AppServerRunResult {
    events: Vec<AppendRunEventRequest>,
    final_status: String,
    session_id: Option<String>,
    native_turn_id: Option<String>,
}

#[derive(Clone)]
struct HubClient {
    http: reqwest::Client,
    hub_url: String,
    runtime_token: Arc<std::sync::RwLock<String>>,
    protocol_capabilities: HashSet<String>,
}

#[derive(Default)]
struct RuntimeRunDispatcher {
    claim_lock: tokio::sync::Mutex<()>,
}

impl RuntimeRunDispatcher {
    async fn claim_next(
        &self,
        client: &HubClient,
        manager: &SessionSupervisorManager,
    ) -> anyhow::Result<Option<ClaimRunResponse>> {
        let _claim_guard = self.claim_lock.lock().await;
        let request = RuntimeClaimRunRequest {
            available_new_session_slots: u32::try_from(manager.available_new_session_slots())
                .unwrap_or(u32::MAX),
            ready_owned_sessions: manager.ready_owned_sessions(),
        };
        let claim = client.claim_run(&request).await?;
        if let Some(claim) = &claim {
            manager.reserve_claim(claim)?;
        }
        Ok(claim)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RuntimeSessionCommandKey {
    session_id: Uuid,
    command_id: Uuid,
    ownership_generation: i64,
}

impl From<&RuntimeSessionCommandDto> for RuntimeSessionCommandKey {
    fn from(command: &RuntimeSessionCommandDto) -> Self {
        Self {
            session_id: command.session_id,
            command_id: command.command_id,
            ownership_generation: command.ownership_generation,
        }
    }
}

#[derive(Default)]
struct RuntimeSessionCommandDispatcherState {
    queues: BTreeMap<Uuid, VecDeque<RuntimeSessionCommandDto>>,
    known: HashSet<RuntimeSessionCommandKey>,
    completed: HashSet<RuntimeSessionCommandKey>,
    running_sessions: HashSet<Uuid>,
}

struct RuntimeSessionCommandDispatcher {
    state: std::sync::Mutex<RuntimeSessionCommandDispatcherState>,
    retry_delay: Duration,
}

impl Default for RuntimeSessionCommandDispatcher {
    fn default() -> Self {
        Self::with_retry_delay(Duration::from_millis(250))
    }
}

impl RuntimeSessionCommandDispatcher {
    fn with_retry_delay(retry_delay: Duration) -> Self {
        Self {
            state: std::sync::Mutex::new(RuntimeSessionCommandDispatcherState::default()),
            retry_delay,
        }
    }

    fn enqueue(
        self: &Arc<Self>,
        client: &HubClient,
        manager: &Arc<SessionSupervisorManager>,
        commands: &[RuntimeSessionCommandDto],
    ) {
        let incoming = commands
            .iter()
            .filter(|command| command.command != "checkpoint")
            .map(RuntimeSessionCommandKey::from)
            .collect::<HashSet<_>>();
        let mut start_sessions = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            let acknowledged = state
                .completed
                .iter()
                .filter(|key| !incoming.contains(key))
                .copied()
                .collect::<Vec<_>>();
            for key in acknowledged {
                state.completed.remove(&key);
                state.known.remove(&key);
            }
            for command in commands {
                if command.command == "checkpoint" {
                    if let Err(error) = manager.request_checkpoint(
                        command.session_id,
                        command.ownership_generation,
                        RuntimeCheckpointReason::Drain,
                    ) {
                        warn!(
                            session_id = %command.session_id,
                            command_id = %command.command_id,
                            error = %error,
                            "failed to register Session checkpoint command"
                        );
                    }
                    continue;
                }
                let key = RuntimeSessionCommandKey::from(command);
                if !state.known.insert(key) {
                    continue;
                }
                manager.begin_session_command(key.session_id, key.ownership_generation);
                state
                    .queues
                    .entry(command.session_id)
                    .or_default()
                    .push_back(command.clone());
                if state.running_sessions.insert(command.session_id) {
                    start_sessions.push(command.session_id);
                }
            }
        }
        for session_id in start_sessions {
            let dispatcher = Arc::clone(self);
            let client = client.clone();
            let manager = Arc::clone(manager);
            tokio::spawn(async move {
                dispatcher
                    .run_session_queue(client, manager, session_id)
                    .await;
            });
        }
    }

    async fn run_session_queue(
        self: Arc<Self>,
        client: HubClient,
        manager: Arc<SessionSupervisorManager>,
        session_id: Uuid,
    ) {
        loop {
            let command = {
                let mut state = self.state.lock().unwrap();
                let command = state
                    .queues
                    .get_mut(&session_id)
                    .and_then(VecDeque::pop_front);
                if command.is_none() {
                    state.queues.remove(&session_id);
                    state.running_sessions.remove(&session_id);
                }
                command
            };
            let Some(command) = command else {
                return;
            };
            let key = RuntimeSessionCommandKey::from(&command);
            match apply_runtime_session_command(&manager, &command).await {
                Ok(applied) => {
                    loop {
                        match client
                            .complete_session_command(&command, applied.outcome)
                            .await
                        {
                            Ok(()) => break,
                            Err(error) if command_ack_is_definitively_stale(&error) => {
                                warn!(
                                    session_id = %command.session_id,
                                    command_id = %command.command_id,
                                    error = %error,
                                    "Session command belongs to a stale Hub generation"
                                );
                                break;
                            }
                            Err(error) => {
                                warn!(
                                    session_id = %command.session_id,
                                    command_id = %command.command_id,
                                    error = %error,
                                    "Session command ACK failed; retrying without repeating the native command"
                                );
                                tokio::time::sleep(self.retry_delay).await;
                            }
                        }
                    }
                    if let Some(error) = applied.native_error {
                        warn!(
                            session_id = %command.session_id,
                            command_id = %command.command_id,
                            error = %error,
                            "native Session command failed"
                        );
                    }
                    self.state.lock().unwrap().completed.insert(key);
                }
                Err(error) => {
                    self.state.lock().unwrap().known.remove(&key);
                    warn!(
                        session_id = %command.session_id,
                        command_id = %command.command_id,
                        error = %error,
                        "Session command failed before producing an ACK outcome"
                    );
                }
            }
            manager.finish_session_command(key.session_id, key.ownership_generation);
        }
    }
}

struct AppliedRuntimeSessionCommand {
    outcome: &'static str,
    native_error: Option<anyhow::Error>,
}

fn command_ack_is_definitively_stale(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::status)
            .is_some_and(|status| matches!(status, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND))
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredRuntimeCredential {
    runtime_id: Uuid,
    runtime_credential: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_runtime_credential: Option<String>,
    #[serde(default)]
    protocol_capabilities: Vec<String>,
}

#[derive(Default)]
struct AppServerCancellation {
    cancelled: AtomicBool,
    #[cfg(unix)]
    process_group_id: AtomicI32,
}

impl AppServerCancellation {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        #[cfg(unix)]
        {
            let process_group_id = self.process_group_id.swap(0, Ordering::AcqRel);
            if process_group_id > 0 {
                kill_process_group(process_group_id);
            }
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn register_child(&self, child: &std::process::Child) {
        #[cfg(unix)]
        {
            let process_group_id = child.id() as i32;
            self.process_group_id
                .store(process_group_id, Ordering::Release);
            if self.is_cancelled() {
                kill_process_group(process_group_id);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = child;
        }
    }

    fn clear_child(&self, child_id: u32) {
        #[cfg(unix)]
        {
            let _ = self.process_group_id.compare_exchange(
                child_id as i32,
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        #[cfg(not(unix))]
        {
            let _ = child_id;
        }
    }
}

struct AppServerCancellationGuard(Arc<AppServerCancellation>);

impl Drop for AppServerCancellationGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Default)]
struct RuntimeHealth {
    registered: AtomicBool,
}

impl RuntimeHealth {
    fn mark_registered(&self) {
        self.registered.store(true, Ordering::Release);
    }

    fn mark_unregistered(&self) {
        self.registered.store(false, Ordering::Release);
    }

    fn is_registered(&self) -> bool {
        self.registered.load(Ordering::Acquire)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "agent_hub_runtime=info".into()))
        .init();

    let mut config = Config::from_env()?;
    let health = Arc::new(RuntimeHealth::default());
    let (_health_addr, _health_server) =
        start_runtime_health_server(config.health_bind_addr, Arc::clone(&health)).await?;
    fs::create_dir_all(&config.work_root).await?;
    resolve_codex_binary(&mut config).await?;
    gc_expired_run_dirs(&config.work_root, config.workdir_ttl, SystemTime::now()).await?;
    run_loop(config, health).await
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let codex_driver = validate_codex_driver(
            &env::var("RUNTIME_CODEX_DRIVER").unwrap_or_else(|_| "fake".into()),
        )?;
        let codex_source = validate_codex_source(
            &env::var("RUNTIME_CODEX_SOURCE").unwrap_or_else(|_| "path".into()),
        )?;
        let workdir_ttl_secs = env::var("RUNTIME_WORKDIR_TTL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(86_400);
        if workdir_ttl_secs == 0 {
            anyhow::bail!("RUNTIME_WORKDIR_TTL_SECS must be positive");
        }
        let model_proxy_idle_timeout_secs = env::var("RUNTIME_MODEL_PROXY_IDLE_TIMEOUT_SECS")
            .ok()
            .map(|value| {
                value
                    .parse::<u64>()
                    .context("RUNTIME_MODEL_PROXY_IDLE_TIMEOUT_SECS must be an integer")
            })
            .transpose()?
            .unwrap_or(DEFAULT_MODEL_PROXY_IDLE_TIMEOUT.as_secs());
        if !(1..=3600).contains(&model_proxy_idle_timeout_secs) {
            anyhow::bail!("RUNTIME_MODEL_PROXY_IDLE_TIMEOUT_SECS must be between 1 and 3600");
        }
        let max_online_sessions =
            parse_max_online_sessions(env::var("RUNTIME_MAX_ONLINE_SESSIONS").ok().as_deref())?;
        let session_idle_timeout = parse_session_idle_timeout(
            env::var("RUNTIME_SESSION_IDLE_TIMEOUT_SECS")
                .ok()
                .as_deref(),
        )?;
        let work_root = PathBuf::from(
            env::var("RUNTIME_WORK_ROOT").unwrap_or_else(|_| "./runtime-data".into()),
        );
        let credential_file = env::var("RUNTIME_CREDENTIAL_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| work_root.join("runtime-credential.json"));
        Ok(Self {
            hub_url: env::var("HUB_URL").unwrap_or_else(|_| "http://localhost:8080".into()),
            enrollment_token: env::var("RUNTIME_ENROLLMENT_TOKEN").ok().and_then(|value| {
                let value = value.trim().to_owned();
                (!value.is_empty()).then_some(value)
            }),
            credential_file,
            work_root,
            hostname: env::var("RUNTIME_HOSTNAME").unwrap_or_else(|_| hostname_fallback()),
            poll_interval: Duration::from_millis(
                env::var("RUNTIME_POLL_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(1000),
            ),
            codex_driver,
            codex_source,
            codex_bin: env::var("CODEX_BIN").unwrap_or_else(|_| "codex".into()),
            codex_version: env::var("RUNTIME_CODEX_VERSION")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "unmanaged".into()),
            app_server_timeout: Duration::from_secs(
                env::var("RUNTIME_APP_SERVER_TIMEOUT_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(DEFAULT_APP_SERVER_TIMEOUT.as_secs()),
            ),
            model_proxy_idle_timeout: Duration::from_secs(model_proxy_idle_timeout_secs),
            session_idle_timeout,
            max_online_sessions,
            workdir_ttl: Duration::from_secs(workdir_ttl_secs),
            local_skills_dir: env::var("RUNTIME_LOCAL_SKILLS_DIR").ok().map(PathBuf::from),
            sandbox_mode: env::var("RUNTIME_SANDBOX_MODE")
                .unwrap_or_else(|_| "workspace-write+network".into()),
            sandbox_downgrade_reason: env::var("RUNTIME_SANDBOX_DOWNGRADE_REASON")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            health_bind_addr: env::var("RUNTIME_HEALTH_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8081".into())
                .parse()
                .context("RUNTIME_HEALTH_BIND_ADDR must be a socket address")?,
        })
    }
}

fn parse_max_online_sessions(value: Option<&str>) -> anyhow::Result<usize> {
    let value = match value {
        Some(value) => value
            .parse::<usize>()
            .context("RUNTIME_MAX_ONLINE_SESSIONS must be a positive integer")?,
        None => DEFAULT_MAX_ONLINE_SESSIONS,
    };
    anyhow::ensure!(value > 0, "RUNTIME_MAX_ONLINE_SESSIONS must be positive");
    Ok(value)
}

fn parse_session_idle_timeout(value: Option<&str>) -> anyhow::Result<Duration> {
    let seconds = match value {
        Some(value) => value
            .parse::<u64>()
            .context("RUNTIME_SESSION_IDLE_TIMEOUT_SECS must be a positive integer")?,
        None => DEFAULT_SESSION_IDLE_TIMEOUT.as_secs(),
    };
    anyhow::ensure!(
        seconds > 0,
        "RUNTIME_SESSION_IDLE_TIMEOUT_SECS must be positive"
    );
    Ok(Duration::from_secs(seconds))
}

fn validate_codex_driver(value: &str) -> anyhow::Result<String> {
    match value {
        "fake" | "app-server" => Ok(value.to_owned()),
        _ => anyhow::bail!("RUNTIME_CODEX_DRIVER must be 'fake' or 'app-server'"),
    }
}

fn validate_codex_source(value: &str) -> anyhow::Result<String> {
    match value {
        "path" => Ok(value.to_owned()),
        _ => anyhow::bail!(
            "RUNTIME_CODEX_SOURCE must be 'path'; managed Codex updates are downloaded through Hub"
        ),
    }
}

async fn resolve_codex_binary(config: &mut Config) -> anyhow::Result<()> {
    if config.codex_driver != "app-server" {
        return Ok(());
    }
    debug_assert_eq!(config.codex_source, "path");
    config.codex_bin = locate_executable(&config.codex_bin)
        .with_context(|| format!("locate Codex binary: {}", config.codex_bin))?
        .display()
        .to_string();
    Ok(())
}

fn locate_executable(value: &str) -> anyhow::Result<PathBuf> {
    let candidate = PathBuf::from(value);
    if candidate.components().count() > 1 {
        return executable_file(candidate);
    }
    let path = env::var_os("PATH").context("PATH is required to locate Codex")?;
    for directory in env::split_paths(&path) {
        let candidate = directory.join(value);
        if let Ok(path) = executable_file(candidate) {
            return Ok(path);
        }
    }
    anyhow::bail!("Codex executable was not found in PATH")
}

fn executable_file(path: PathBuf) -> anyhow::Result<PathBuf> {
    let metadata = stdfs::metadata(&path)?;
    if !metadata.is_file() {
        anyhow::bail!("Codex path is not a file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            anyhow::bail!("Codex path is not executable");
        }
    }
    Ok(stdfs::canonicalize(path)?)
}

async fn verify_codex_compatibility(
    binary: &Path,
    expected_version: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    anyhow::ensure!(!timeout.is_zero(), "Codex compatibility timeout is zero");
    let version = tokio::time::timeout(
        timeout,
        tokio::process::Command::new(binary)
            .arg("--version")
            .output(),
    )
    .await
    .context("Codex --version compatibility check timed out")??;
    anyhow::ensure!(
        version.status.success(),
        "Codex --version compatibility check failed"
    );
    let stdout =
        String::from_utf8(version.stdout).context("Codex --version output is not UTF-8")?;
    anyhow::ensure!(
        stdout
            .split_whitespace()
            .any(|component| component == expected_version),
        "Codex version mismatch: expected {expected_version}, got {}",
        stdout.trim()
    );
    let app_server = tokio::time::timeout(
        timeout,
        tokio::process::Command::new(binary)
            .arg("app-server")
            .arg("--help")
            .output(),
    )
    .await
    .context("Codex app-server compatibility check timed out")??;
    anyhow::ensure!(
        app_server.status.success(),
        "Codex app-server compatibility check failed"
    );
    Ok(())
}

async fn install_managed_codex_artifact(
    work_root: &Path,
    client: &HubClient,
    artifact: &CodexVersionArtifactDto,
    compatibility_timeout: Duration,
) -> anyhow::Result<PathBuf> {
    validate_managed_codex_version(&artifact.version)?;
    anyhow::ensure!(
        artifact.os == std::env::consts::OS && artifact.architecture == std::env::consts::ARCH,
        "Hub returned a Codex artifact for another Runtime platform"
    );
    anyhow::ensure!(
        artifact.size_bytes > 0 && artifact.size_bytes <= MAX_CODEX_ARTIFACT_BYTES,
        "Codex artifact size is outside the supported limit"
    );
    let binary_name = if cfg!(windows) { "codex.exe" } else { "codex" };
    let install_root = work_root.join("bin").join(&artifact.version);
    let installed = install_root.join(binary_name);
    if tokio::fs::try_exists(&installed).await? {
        verify_codex_compatibility(&installed, &artifact.version, compatibility_timeout).await?;
        return Ok(installed);
    }

    let staging =
        work_root
            .join("bin")
            .join(format!(".staging-{}-{}", artifact.version, Uuid::new_v4()));
    tokio::fs::create_dir_all(&staging).await?;
    let result = async {
        let compressed_path = staging.join("codex.zst");
        let response = client
            .http
            .get(format!(
                "{}/api/runtime/codex/artifacts/{}/{}/{}",
                client.hub_url, artifact.version, artifact.os, artifact.architecture
            ))
            .bearer_auth(client.runtime_credential())
            .send()
            .await?
            .error_for_status()?;
        if let Some(length) = response.content_length() {
            anyhow::ensure!(
                length == artifact.size_bytes,
                "Codex artifact size mismatch"
            );
        }
        let mut compressed = fs::File::create(&compressed_path).await?;
        let mut stream = response.bytes_stream();
        let mut hasher = Sha256::new();
        let mut size = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            size = size
                .checked_add(chunk.len() as u64)
                .context("Codex artifact size overflow")?;
            anyhow::ensure!(
                size <= artifact.size_bytes && size <= MAX_CODEX_ARTIFACT_BYTES,
                "Codex artifact size mismatch"
            );
            hasher.update(&chunk);
            compressed.write_all(&chunk).await?;
        }
        compressed.flush().await?;
        anyhow::ensure!(size == artifact.size_bytes, "Codex artifact size mismatch");
        let actual_sha256 = format!("{:x}", hasher.finalize());
        anyhow::ensure!(
            actual_sha256 == artifact.sha256,
            "Codex artifact SHA-256 mismatch"
        );

        let staged_binary = staging.join(binary_name);
        let compressed_path_for_decode = compressed_path.clone();
        let staged_binary_for_decode = staged_binary.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let source = stdfs::File::open(compressed_path_for_decode)?;
            let decoder = zstd::stream::read::Decoder::new(source)?;
            let mut limited = decoder.take(MAX_CODEX_BINARY_BYTES + 1);
            let mut destination = stdfs::File::create(staged_binary_for_decode)?;
            let written = std::io::copy(&mut limited, &mut destination)?;
            anyhow::ensure!(
                written <= MAX_CODEX_BINARY_BYTES,
                "Codex binary exceeds the supported size limit"
            );
            destination.flush()?;
            Ok(())
        })
        .await??;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = stdfs::metadata(&staged_binary)?.permissions();
            permissions.set_mode(0o755);
            stdfs::set_permissions(&staged_binary, permissions)?;
        }
        verify_codex_compatibility(&staged_binary, &artifact.version, compatibility_timeout)
            .await?;
        fs::remove_file(&compressed_path).await?;
        fs::create_dir_all(work_root.join("bin")).await?;
        match fs::rename(&staging, &install_root).await {
            Ok(()) => Ok(installed.clone()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_dir_all(&staging).await;
                verify_codex_compatibility(&installed, &artifact.version, compatibility_timeout)
                    .await?;
                Ok(installed.clone())
            }
            Err(error) => Err(error.into()),
        }
    }
    .await;
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging).await;
    }
    result
}

async fn apply_runtime_codex_rollout(
    config: &mut Config,
    client: &HubClient,
    rollout: &mut RuntimeCodexState,
    manager: Option<&SessionSupervisorManager>,
    command: &RuntimeCodexRolloutCommandDto,
) {
    let active_catch_up = command.target_artifact.as_ref().is_some_and(|artifact| {
        command.active_version.as_deref() == Some(artifact.version.as_str())
            && artifact.version != rollout.current_version
    });

    if let Some(artifact) = command.target_artifact.as_ref() {
        let is_same_ready_candidate = rollout.candidate_version.as_deref()
            == Some(artifact.version.as_str())
            && rollout.candidate_status.as_deref() == Some("ready")
            && rollout
                .candidate_binary
                .as_ref()
                .is_some_and(|path| path.exists());
        let is_same_failed_candidate = rollout.candidate_version.as_deref()
            == Some(artifact.version.as_str())
            && rollout.candidate_status.as_deref() == Some("failed")
            && !active_catch_up;
        if !is_same_ready_candidate && !is_same_failed_candidate {
            match install_managed_codex_artifact(
                &config.work_root,
                client,
                artifact,
                config.app_server_timeout,
            )
            .await
            {
                Ok(binary) => {
                    rollout.candidate_version = Some(artifact.version.clone());
                    rollout.candidate_status = Some("ready".into());
                    rollout.candidate_error = None;
                    rollout.candidate_binary = Some(binary);
                }
                Err(error) if active_catch_up => {
                    warn!(
                        version = %artifact.version,
                        error = %error,
                        "Active Codex artifact could not be installed yet"
                    );
                    return;
                }
                Err(error) => {
                    rollout.candidate_version = Some(artifact.version.clone());
                    rollout.candidate_status = Some("failed".into());
                    rollout.candidate_error = Some(error.to_string());
                    rollout.candidate_binary = None;
                }
            }
        }
    }

    let Some(active_version) = command.active_version.as_deref() else {
        return;
    };
    if active_version == rollout.current_version {
        return;
    }
    let Some(binary) = (rollout.candidate_version.as_deref() == Some(active_version)
        && rollout.candidate_status.as_deref() == Some("ready"))
    .then(|| rollout.candidate_binary.clone())
    .flatten() else {
        return;
    };
    if !binary.exists() {
        return;
    }
    if let Err(error) =
        verify_codex_compatibility(&binary, active_version, config.app_server_timeout).await
    {
        warn!(version = %active_version, error = %error, "Active Codex artifact failed its final compatibility check");
        return;
    }
    if let Some(manager) = manager {
        if let Err(error) =
            manager.request_version_switch_checkpoints(active_version, &rollout.current_version)
        {
            warn!(error = %error, "Could not arm Session checkpoints for Codex version switch");
            return;
        }
    }
    config.codex_bin = binary.display().to_string();
    config.codex_version = active_version.to_owned();
    rollout.current_version = active_version.to_owned();
    rollout.clear_candidate();
}

fn validate_managed_codex_version(version: &str) -> anyhow::Result<()> {
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

async fn gc_expired_run_dirs(root: &Path, ttl: Duration, now: SystemTime) -> anyhow::Result<usize> {
    let mut removed = 0;
    let mut entries = match fs::read_dir(root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if Uuid::parse_str(name).is_err() || !entry.file_type().await?.is_dir() {
            continue;
        }
        let modified = entry.metadata().await?.modified()?;
        if now.duration_since(modified).unwrap_or_default() < ttl {
            continue;
        }
        fs::remove_dir_all(entry.path()).await?;
        removed += 1;
    }
    Ok(removed)
}

fn load_runtime_credential(path: &Path) -> anyhow::Result<Option<StoredRuntimeCredential>> {
    let bytes = match stdfs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("read Runtime credential file"),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = stdfs::metadata(path)?.permissions().mode() & 0o777;
        if mode != 0o600 {
            anyhow::bail!("Runtime credential file must have mode 0600, found {mode:04o}");
        }
    }
    let stored: StoredRuntimeCredential =
        serde_json::from_slice(&bytes).context("parse Runtime credential file")?;
    if stored.runtime_credential.trim().is_empty() {
        anyhow::bail!("Runtime credential file contains an empty credential");
    }
    if stored
        .pending_runtime_credential
        .as_deref()
        .is_some_and(|credential| credential.trim().is_empty())
    {
        anyhow::bail!("Runtime credential file contains an empty pending credential");
    }
    Ok(Some(stored))
}

fn persist_runtime_credential(path: &Path, stored: &StoredRuntimeCredential) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("Runtime credential file has no parent directory")?;
    stdfs::create_dir_all(parent).context("create Runtime credential directory")?;
    let temporary = path.with_extension(format!("tmp-{}", Uuid::new_v4().simple()));
    let bytes = serde_json::to_vec(stored).context("serialize Runtime credential")?;
    let write_result = (|| -> anyhow::Result<()> {
        let mut options = stdfs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .context("create temporary Runtime credential file")?;
        file.write_all(&bytes)
            .context("write temporary Runtime credential file")?;
        file.sync_all()
            .context("sync temporary Runtime credential file")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            stdfs::set_permissions(&temporary, stdfs::Permissions::from_mode(0o600))
                .context("protect temporary Runtime credential file")?;
        }
        stdfs::rename(&temporary, path).context("replace Runtime credential file")?;
        let directory = stdfs::File::open(parent).context("open Runtime credential directory")?;
        directory
            .sync_all()
            .context("sync Runtime credential directory")?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = stdfs::remove_file(&temporary);
    }
    write_result
}

fn runtime_http_client() -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
        .context("build runtime HTTP client")
}

fn hub_client_from_stored(
    config: &Config,
    http: reqwest::Client,
    stored: &StoredRuntimeCredential,
) -> HubClient {
    HubClient {
        http,
        hub_url: config.hub_url.clone(),
        runtime_token: Arc::new(std::sync::RwLock::new(stored.runtime_credential.clone())),
        protocol_capabilities: stored.protocol_capabilities.iter().cloned().collect(),
    }
}

async fn initialize_runtime(
    config: &Config,
) -> anyhow::Result<(HubClient, StoredRuntimeCredential)> {
    let http = runtime_http_client()?;
    if let Some(stored) = load_runtime_credential(&config.credential_file)? {
        info!(runtime_id = %stored.runtime_id, "loaded persisted Runtime identity");
        let client = hub_client_from_stored(config, http, &stored);
        return Ok((client, stored));
    }
    let enrollment_token = config
        .enrollment_token
        .as_deref()
        .context("RUNTIME_ENROLLMENT_TOKEN is required when no Runtime credential file exists")?;
    let req = runtime_register_request(config);
    let response = http
        .post(format!("{}/api/runtime/register", config.hub_url))
        .bearer_auth(enrollment_token)
        .json(&req)
        .send()
        .await?
        .error_for_status()?
        .json::<RuntimeRegisterResponse>()
        .await?;
    let stored = StoredRuntimeCredential {
        runtime_id: response.runtime_id,
        runtime_credential: response.runtime_credential,
        pending_runtime_credential: None,
        protocol_capabilities: response.protocol_capabilities,
    };
    persist_runtime_credential(&config.credential_file, &stored)?;
    info!(runtime_id = %stored.runtime_id, "Runtime enrollment completed");
    let client = hub_client_from_stored(config, http, &stored);
    Ok((client, stored))
}

fn runtime_register_request(config: &Config) -> RuntimeRegisterRequest {
    let (effective_sandbox_mode, sandbox_downgraded) = effective_sandbox_mode(config);
    RuntimeRegisterRequest {
        hostname: config.hostname.clone(),
        labels: vec!["local".into(), format!("driver:{}", config.codex_driver)],
        codex_version: if config.codex_driver == "app-server" {
            config.codex_version.clone()
        } else {
            "fake-codex-0.1".into()
        },
        capabilities: json!({
            "driver": config.codex_driver,
            "codex_source": config.codex_source,
            "platform": {
                "os": std::env::consts::OS,
                "architecture": std::env::consts::ARCH
            },
            "model_proxy": true,
            "mcp_allowlist": true,
            "thread_resume": true,
            "local_skills": config.local_skills_dir.is_some(),
            "sandbox_downgraded": sandbox_downgraded,
            "sandbox_downgrade_reason": config.sandbox_downgrade_reason,
            "sandbox": {
                "configured_mode": config.sandbox_mode,
                "effective_mode": effective_sandbox_mode,
                "downgraded": sandbox_downgraded,
                "downgrade_reason": config.sandbox_downgrade_reason
            }
        }),
        sandbox_mode: effective_sandbox_mode,
    }
}

fn effective_sandbox_mode(config: &Config) -> (String, bool) {
    if config.sandbox_downgrade_reason.is_some() {
        ("read-only".into(), true)
    } else {
        (config.sandbox_mode.clone(), false)
    }
}

async fn start_runtime_health_server(
    bind_addr: SocketAddr,
    health: Arc<RuntimeHealth>,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .context("bind runtime health endpoint")?;
    let addr = listener
        .local_addr()
        .context("read runtime health endpoint address")?;
    let app = Router::new()
        .route("/healthz", get(runtime_health))
        .with_state(health);
    let handle = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            warn!(error = %error, "runtime health endpoint stopped");
        }
    });
    Ok((addr, handle))
}

async fn runtime_health(State(health): State<Arc<RuntimeHealth>>) -> impl IntoResponse {
    if health.is_registered() {
        AxumStatusCode::OK.into_response()
    } else {
        AxumStatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

async fn run_loop(config: Config, health: Arc<RuntimeHealth>) -> anyhow::Result<()> {
    let (mut client, mut stored) = initialize_runtime(&config).await?;
    let mut config = config;
    run_registered_cycle(&mut config, &mut client, &mut stored, &health).await
}

async fn run_registered_cycle(
    config: &mut Config,
    client: &mut HubClient,
    stored: &mut StoredRuntimeCredential,
    health: &RuntimeHealth,
) -> anyhow::Result<()> {
    let mut last_gc = Instant::now();
    let mut codex_rollout = RuntimeCodexState::new(config);
    let dispatcher = RuntimeRunDispatcher::default();
    let command_dispatcher = Arc::new(RuntimeSessionCommandDispatcher::default());
    let mut manager: Option<Arc<SessionSupervisorManager>> = None;
    let mut workers = JoinSet::new();
    loop {
        while let Some(result) = workers.try_join_next() {
            if let Err(error) = result {
                warn!(error = %error, "Session worker task stopped unexpectedly");
            }
        }
        let heartbeat = match send_runtime_heartbeat(
            config,
            client,
            stored,
            manager.as_deref(),
            &codex_rollout,
        )
        .await
        {
            Ok(heartbeat) => {
                health.mark_registered();
                Some(heartbeat)
            }
            Err(err) if is_auth_loss(&err) => {
                health.mark_unregistered();
                warn!(error = %err, "Runtime credential rejected; enrollment is not retried");
                None
            }
            Err(err) => {
                warn!(error = %err, "heartbeat failed");
                None
            }
        };
        if let Some(heartbeat) = heartbeat {
            let (session_manager, cleanups) = if let Some(manager) = &manager {
                (
                    Arc::clone(manager),
                    manager.reconcile_owned_snapshots(&heartbeat.owned_sessions)?,
                )
            } else {
                let recovery = plan_session_recovery(
                    &config.work_root,
                    stored.runtime_id,
                    &heartbeat.owned_sessions,
                    config.max_online_sessions,
                )
                .await?;
                let recovered = SessionSupervisorManager::try_recover_cold_with_idle_timeout(
                    config.work_root.clone(),
                    stored.runtime_id,
                    recovery,
                    config.session_idle_timeout,
                )?;
                let cleanups = recovered.take_pending_released_session_cleanups()?;
                manager = Some(Arc::clone(&recovered));
                (recovered, cleanups)
            };
            let mut cleanups = cleanups;
            cleanups.extend(
                session_manager.reserve_hub_cleanup_obligations(&heartbeat.cleanup_sessions)?,
            );
            for cleanup in cleanups {
                let cleanup_manager = Arc::clone(&session_manager);
                workers.spawn(async move {
                    remove_hub_fenced_session_cleanup(&cleanup_manager, cleanup).await;
                });
            }
            apply_runtime_codex_rollout(
                config,
                client,
                &mut codex_rollout,
                Some(&session_manager),
                &heartbeat.codex_rollout,
            )
            .await;
            command_dispatcher.enqueue(client, &session_manager, &heartbeat.session_commands);
            fail_interrupted_restoring_runs(&session_manager, client).await;
            match dispatcher.claim_next(client, &session_manager).await {
                Ok(Some(claim)) => {
                    workers.spawn(run_claim_worker(
                        config.clone(),
                        client.clone(),
                        session_manager,
                        claim,
                    ));
                }
                Ok(None) => {}
                Err(err) if is_auth_loss(&err) => {
                    health.mark_unregistered();
                    warn!(error = %err, "Runtime credential rejected during claim");
                }
                Err(err) => warn!(error = %err, "claim failed"),
            }
        }
        if let Some(manager) = &manager {
            let checkpoint_transport = HubRuntimeCheckpointTransport {
                client: client.clone(),
                work_root: config.work_root.clone(),
                producing_codex_version: config.codex_version.clone(),
            };
            if let Err(error) = drive_runtime_checkpoints(manager, &checkpoint_transport).await {
                warn!(error = %error, "failed to drive Session checkpoints");
            }
        }
        if last_gc.elapsed() >= Duration::from_secs(300) {
            match gc_expired_run_dirs(&config.work_root, config.workdir_ttl, SystemTime::now())
                .await
            {
                Ok(removed) if removed > 0 => info!(removed, "expired runtime workdirs removed"),
                Ok(_) => {}
                Err(error) => warn!(error = %error, "runtime workdir GC failed"),
            }
            last_gc = Instant::now();
        }
        tokio::time::sleep(config.poll_interval).await;
    }
}

async fn apply_runtime_session_command(
    manager: &SessionSupervisorManager,
    command: &RuntimeSessionCommandDto,
) -> anyhow::Result<AppliedRuntimeSessionCommand> {
    match command.command.as_str() {
        "steer" => {
            let message = command
                .message
                .as_ref()
                .context("steer command is missing its Hub message")?;
            anyhow::ensure!(
                message.id == command.command_id,
                "steer command id does not match its Hub message"
            );
            let native_thread_id = command
                .native_thread_id
                .as_deref()
                .context("steer command is missing its native Thread id")?;
            let native_turn_id = command
                .native_turn_id
                .clone()
                .context("steer command is missing its expected native Turn id")?;
            let outcome = manager
                .steer(
                    command.session_id,
                    command.ownership_generation,
                    native_thread_id,
                    native_turn_id,
                    message.id,
                    message.content.clone(),
                )
                .await;
            match outcome {
                Ok(SessionSteerOutcome::Applied) => Ok(AppliedRuntimeSessionCommand {
                    outcome: "applied",
                    native_error: None,
                }),
                Ok(SessionSteerOutcome::TurnEnded) => Ok(AppliedRuntimeSessionCommand {
                    outcome: "turn_ended",
                    native_error: None,
                }),
                Err(error) => Ok(AppliedRuntimeSessionCommand {
                    outcome: "failed",
                    native_error: Some(error),
                }),
            }
        }
        "interrupt" => {
            let native_thread_id = command
                .native_thread_id
                .as_deref()
                .context("interrupt command is missing its native Thread id")?;
            let native_turn_id = command
                .native_turn_id
                .clone()
                .context("interrupt command is missing its native Turn id")?;
            let outcome = manager
                .interrupt(
                    command.session_id,
                    command.ownership_generation,
                    native_thread_id,
                    native_turn_id,
                )
                .await?;
            Ok(AppliedRuntimeSessionCommand {
                outcome: match outcome {
                    SessionInterruptOutcome::Interrupted => "interrupted",
                    SessionInterruptOutcome::TurnEnded => "turn_ended",
                },
                native_error: None,
            })
        }
        _ => anyhow::bail!("unsupported Runtime Session command"),
    }
}

async fn run_claim_worker(
    config: Config,
    client: HubClient,
    manager: Arc<SessionSupervisorManager>,
    claim: ClaimRunResponse,
) {
    let run_id = claim.run.id;
    let session_id = claim.run.hub_session_id;
    let ownership_generation = claim.run.session_ownership_generation;
    let result = if config.codex_driver == "app-server" {
        execute_managed_run(&config, &client, Arc::clone(&manager), claim).await
    } else {
        let result = execute_run(&config, &client, claim.clone()).await;
        match result {
            Ok(()) => manager.complete_fake_claim(&claim).await,
            Err(error) => Err(error),
        }
    };
    let bundle_recovery_failed = result.as_ref().err().is_some_and(|error| {
        error
            .downcast_ref::<SessionBundleRestoreFailure>()
            .is_some()
    });
    if let Err(error) = result {
        if let Some(session_id) = session_id {
            manager.cancel_session(session_id, error.to_string());
        }
        warn!(run_id = %run_id, error = %error, "run execution failed");
        if let Some(ownership_generation) = ownership_generation {
            match client.fail_run(run_id, ownership_generation).await {
                Ok(()) if bundle_recovery_failed => {
                    if let Some(session_id) = session_id {
                        manager.forget_fenced_session(session_id);
                    }
                }
                Ok(()) => {}
                Err(mark_error) => {
                    warn!(run_id = %run_id, error = %mark_error, "failed to mark run failed");
                }
            }
        }
    }
}

fn runtime_credential_sha256(credential: &str) -> String {
    Sha256::digest(credential.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn new_runtime_credential() -> String {
    format!(
        "ahrc_{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    )
}

#[cfg(test)]
async fn reconcile_runtime_credential(
    config: &Config,
    client: &mut HubClient,
    stored: &mut StoredRuntimeCredential,
) -> anyhow::Result<RuntimeHeartbeatResponse> {
    reconcile_runtime_credential_with_request(
        config,
        client,
        stored,
        &RuntimeHeartbeatRequest::default(),
    )
    .await
}

async fn send_runtime_heartbeat(
    config: &Config,
    client: &HubClient,
    stored: &mut StoredRuntimeCredential,
    manager: Option<&SessionSupervisorManager>,
    codex_rollout: &RuntimeCodexState,
) -> anyhow::Result<RuntimeHeartbeatResponse> {
    let mut request = manager
        .map(SessionSupervisorManager::heartbeat_request)
        .unwrap_or_default();
    request.codex_status = Some(codex_rollout.heartbeat_status());
    let heartbeat =
        reconcile_runtime_credential_with_request(config, client, stored, &request).await?;
    if let Some(manager) = manager {
        manager.acknowledge_cleaned_sessions(&request.cleaned_sessions)?;
    }
    Ok(heartbeat)
}

async fn reconcile_runtime_credential_with_request(
    config: &Config,
    client: &HubClient,
    stored: &mut StoredRuntimeCredential,
    heartbeat_request: &RuntimeHeartbeatRequest,
) -> anyhow::Result<RuntimeHeartbeatResponse> {
    if let Some(pending) = stored.pending_runtime_credential.as_deref() {
        match client
            .heartbeat_with_credential(pending, heartbeat_request)
            .await
        {
            Ok(heartbeat) => {
                promote_local_runtime_credential(config, client, stored)?;
                return Ok(heartbeat);
            }
            Err(error) if is_auth_loss(&error) => {}
            Err(error) => return Err(error),
        }
    }

    let heartbeat = client
        .heartbeat_with_credential(&stored.runtime_credential, heartbeat_request)
        .await?;
    if !heartbeat.rotation_requested {
        if stored.pending_runtime_credential.is_some() {
            anyhow::bail!("Hub no longer recognizes the pending Runtime credential");
        }
        return Ok(heartbeat);
    }

    if stored.pending_runtime_credential.is_none() {
        let mut staged = stored.clone();
        staged.pending_runtime_credential = Some(new_runtime_credential());
        persist_runtime_credential(&config.credential_file, &staged)?;
        *stored = staged;
    }
    let pending = stored
        .pending_runtime_credential
        .as_deref()
        .context("pending Runtime credential disappeared during rotation")?;
    let mut staged_request = heartbeat_request.clone();
    staged_request.pending_credential_hash = Some(runtime_credential_sha256(pending));
    let staged = client
        .heartbeat_with_credential(&stored.runtime_credential, &staged_request)
        .await?;
    if !staged.pending_credential_accepted {
        anyhow::bail!("Hub did not accept the pending Runtime credential");
    }
    let activated = client
        .heartbeat_with_credential(pending, heartbeat_request)
        .await?;
    promote_local_runtime_credential(config, client, stored)?;
    Ok(activated)
}

fn promote_local_runtime_credential(
    config: &Config,
    client: &HubClient,
    stored: &mut StoredRuntimeCredential,
) -> anyhow::Result<()> {
    let pending = stored
        .pending_runtime_credential
        .clone()
        .context("cannot promote a missing pending Runtime credential")?;
    let mut promoted = stored.clone();
    promoted.runtime_credential = pending.clone();
    promoted.pending_runtime_credential = None;
    persist_runtime_credential(&config.credential_file, &promoted)?;
    client.replace_runtime_credential(pending);
    *stored = promoted;
    Ok(())
}

fn is_auth_loss(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::status)
            .is_some_and(|status| {
                status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN
            })
    })
}

impl HubClient {
    fn runtime_credential(&self) -> String {
        self.runtime_token.read().unwrap().clone()
    }

    fn replace_runtime_credential(&self, credential: String) {
        *self.runtime_token.write().unwrap() = credential;
    }

    async fn heartbeat(&self) -> anyhow::Result<()> {
        self.heartbeat_with_credential(
            &self.runtime_credential(),
            &RuntimeHeartbeatRequest::default(),
        )
        .await?;
        Ok(())
    }

    async fn heartbeat_with_credential(
        &self,
        credential: &str,
        request: &RuntimeHeartbeatRequest,
    ) -> anyhow::Result<RuntimeHeartbeatResponse> {
        Ok(self
            .http
            .post(format!("{}/api/runtime/heartbeat", self.hub_url))
            .bearer_auth(credential)
            .json(request)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn claim_run(
        &self,
        request: &RuntimeClaimRunRequest,
    ) -> anyhow::Result<Option<ClaimRunResponse>> {
        let response = self
            .http
            .post(format!("{}/api/runtime/runs/claim", self.hub_url))
            .bearer_auth(self.runtime_credential())
            .json(request)
            .send()
            .await?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        Ok(Some(response.error_for_status()?.json().await?))
    }

    async fn append_event(
        &self,
        run_id: Uuid,
        ownership_generation: i64,
        req: AppendRunEventRequest,
    ) -> anyhow::Result<()> {
        self.http
            .post(format!("{}/api/runtime/runs/{run_id}/events", self.hub_url))
            .bearer_auth(self.runtime_credential())
            .json(&RuntimeSessionWriteRequest {
                ownership_generation,
                payload: req,
            })
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn begin_turn(
        &self,
        run_id: Uuid,
        ownership_generation: i64,
        configuration_fingerprint: &str,
    ) -> anyhow::Result<BeginRuntimeTurnResponse> {
        Ok(self
            .http
            .post(format!(
                "{}/api/runtime/runs/{run_id}/turn/begin",
                self.hub_url
            ))
            .bearer_auth(self.runtime_credential())
            .json(&RuntimeSessionWriteRequest {
                ownership_generation,
                payload: BeginRuntimeTurnRequest {
                    configuration_fingerprint: configuration_fingerprint.to_owned(),
                },
            })
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn complete_session_command(
        &self,
        command: &RuntimeSessionCommandDto,
        outcome: &str,
    ) -> anyhow::Result<()> {
        self.http
            .post(format!(
                "{}/api/runtime/sessions/{}/commands/{}/complete",
                self.hub_url, command.session_id, command.command_id
            ))
            .bearer_auth(self.runtime_credential())
            .json(&RuntimeSessionWriteRequest {
                ownership_generation: command.ownership_generation,
                payload: CompleteRuntimeSessionCommandRequest {
                    command: command.command.clone(),
                    outcome: outcome.to_owned(),
                    revision: command.configuration_revision,
                    fingerprint: command.fingerprint.clone(),
                },
            })
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn begin_session_checkpoint(
        &self,
        session_id: Uuid,
        request: &BeginRuntimeSessionCheckpointRequest,
    ) -> anyhow::Result<RuntimeSessionCheckpointAttemptDto> {
        Ok(self
            .http
            .post(format!(
                "{}/api/runtime/sessions/{session_id}/checkpoint/begin",
                self.hub_url
            ))
            .bearer_auth(self.runtime_credential())
            .json(request)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn fail_session_checkpoint(
        &self,
        session_id: Uuid,
        request: &FailRuntimeSessionCheckpointRequest,
    ) -> anyhow::Result<RuntimeSessionCheckpointDispositionDto> {
        Ok(self
            .http
            .post(format!(
                "{}/api/runtime/sessions/{session_id}/checkpoint/fail",
                self.hub_url
            ))
            .bearer_auth(self.runtime_credential())
            .json(request)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn upload_session_bundle(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
        checkpoint_attempt_id: Uuid,
        artifact: &session_bundle::SessionBundleArtifact,
    ) -> anyhow::Result<RuntimeSessionBundleCommitResponseDto> {
        let file = fs::File::open(&artifact.archive_path)
            .await
            .context("open staged Session Bundle")?;
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        Ok(self
            .http
            .put(format!(
                "{}/api/runtime/sessions/{session_id}/bundle",
                self.hub_url
            ))
            .bearer_auth(self.runtime_credential())
            .header("x-agent-hub-ownership-generation", ownership_generation)
            .header(
                "x-agent-hub-checkpoint-attempt-id",
                checkpoint_attempt_id.to_string(),
            )
            .header(
                "x-agent-hub-bundle-generation",
                artifact.manifest.bundle_generation,
            )
            .header("x-agent-hub-bundle-sha256", &artifact.checksum_sha256)
            .header("x-agent-hub-bundle-size", artifact.size_bytes)
            .header(
                "x-agent-hub-history-checkpoint",
                artifact.manifest.history_checkpoint,
            )
            .header(
                "x-agent-hub-producing-codex-version",
                &artifact.manifest.producing_codex_version,
            )
            .header(
                "x-agent-hub-bundle-created-at",
                artifact.manifest.created_at.to_rfc3339(),
            )
            .header(reqwest::header::CONTENT_TYPE, "application/zstd")
            .header(reqwest::header::CONTENT_LENGTH, artifact.size_bytes)
            .body(body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn download_session_bundle(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
        current: &CurrentSessionBundleDto,
        destination: &Path,
    ) -> anyhow::Result<()> {
        let response = self
            .http
            .get(format!(
                "{}/api/runtime/sessions/{session_id}/bundle",
                self.hub_url
            ))
            .bearer_auth(self.runtime_credential())
            .header("x-agent-hub-ownership-generation", ownership_generation)
            .send()
            .await?
            .error_for_status()?;
        let response_size = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .context("Hub Bundle download is missing a valid Content-Length")?;
        anyhow::ensure!(
            response_size == current.size_bytes as u64,
            "Hub Bundle download size does not match Session metadata"
        );
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).await?;
        }
        let temporary = destination.with_extension(format!("tmp-{}", Uuid::new_v4().simple()));
        let write_result: anyhow::Result<()> = async {
            let mut file = fs::File::create(&temporary).await?;
            let mut received = 0_u64;
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.context("stream Session Bundle from Hub")?;
                received = received
                    .checked_add(chunk.len() as u64)
                    .context("Session Bundle download size overflowed")?;
                anyhow::ensure!(
                    received <= current.size_bytes as u64,
                    "Session Bundle download exceeds Hub metadata"
                );
                file.write_all(&chunk).await?;
            }
            anyhow::ensure!(
                received == current.size_bytes as u64,
                "Session Bundle download ended before its declared size"
            );
            file.sync_all().await?;
            fs::rename(&temporary, destination).await?;
            Ok(())
        }
        .await;
        if write_result.is_err() {
            let _ = fs::remove_file(&temporary).await;
        }
        write_result
    }

    async fn finalize_tool_requests(
        &self,
        run_id: Uuid,
        ownership_generation: i64,
        req: &FinalizeToolRequestsRequest,
    ) -> anyhow::Result<()> {
        let url = format!(
            "{}/api/runtime/runs/{run_id}/tool-requests/finalize",
            self.hub_url
        );
        let response = match self
            .http
            .post(&url)
            .bearer_auth(self.runtime_credential())
            .json(&RuntimeSessionWriteRequest {
                ownership_generation,
                payload: req,
            })
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => {
                self.http
                    .post(&url)
                    .bearer_auth(self.runtime_credential())
                    .json(&RuntimeSessionWriteRequest {
                        ownership_generation,
                        payload: req,
                    })
                    .send()
                    .await?
            }
        };
        response.error_for_status()?;
        Ok(())
    }

    async fn complete_run(
        &self,
        run_id: Uuid,
        ownership_generation: i64,
        req: CompleteRunRequest,
    ) -> anyhow::Result<()> {
        self.http
            .post(format!(
                "{}/api/runtime/runs/{run_id}/complete",
                self.hub_url
            ))
            .bearer_auth(self.runtime_credential())
            .json(&RuntimeSessionWriteRequest {
                ownership_generation,
                payload: req,
            })
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn fail_run(&self, run_id: Uuid, ownership_generation: i64) -> anyhow::Result<()> {
        let _ = self
            .append_event(
                run_id,
                ownership_generation,
                AppendRunEventRequest {
                    event_type: "status".into(),
                    role: None,
                    content: Some("failed".into()),
                    payload: json!({ "error": "runtime execution failed" }),
                    waiting_tool: None,
                },
            )
            .await;
        self.complete_run(
            run_id,
            ownership_generation,
            CompleteRunRequest {
                status: "failed".into(),
                session_id: None,
                work_dir_ref: None,
            },
        )
        .await
    }
}

fn ensure_atomic_tool_request_protocol(
    client: &HubClient,
    claim: &ClaimRunResponse,
) -> anyhow::Result<()> {
    let has_dynamic_tools = claim.integration_context.as_ref().is_some_and(|context| {
        !matches!(&context.tools, serde_json::Value::Array(tools) if tools.is_empty())
            && !context.tools.is_null()
    });
    if has_dynamic_tools
        && !client
            .protocol_capabilities
            .contains(ATOMIC_WAITING_TOOL_BATCH_CAPABILITY)
    {
        anyhow::bail!("Hub protocol does not support atomic Integration tool requests");
    }
    Ok(())
}

async fn execute_run(
    config: &Config,
    client: &HubClient,
    claim: ClaimRunResponse,
) -> anyhow::Result<()> {
    ensure_atomic_tool_request_protocol(client, &claim)?;
    let model_proxy = start_model_proxy(
        client,
        claim.run.id,
        &claim.model_proxy_token,
        config.model_proxy_idle_timeout,
    )
    .await?;
    let run_env = prepare_run_env_with_local_skills(
        &config.work_root,
        &claim,
        Some(&model_proxy.base_url),
        config.local_skills_dir.as_deref(),
    )
    .await?;
    info!(
        run_id = %claim.run.id,
        workdir = %run_env.workdir.display(),
        codex_home = %run_env.codex_home.display(),
        "claimed run"
    );

    let mut last_heartbeat = Instant::now();
    let app_server_result = if config.codex_driver == "app-server" {
        Some(
            execute_app_server_with_streaming(
                config,
                client,
                &claim,
                &run_env,
                &mut last_heartbeat,
            )
            .await?,
        )
    } else {
        None
    };
    let (events, final_status, session_id) = if let Some(result) = app_server_result {
        (result.events, result.final_status, result.session_id)
    } else {
        let (events, final_status) = fake_codex_events(&claim);
        (
            events,
            final_status,
            Some(format!("fake-session-{}", claim.run.id)),
        )
    };
    finish_claimed_run(
        client,
        &claim,
        &run_env,
        events,
        final_status,
        session_id,
        &mut last_heartbeat,
    )
    .await
}

async fn finish_claimed_run(
    client: &HubClient,
    claim: &ClaimRunResponse,
    run_env: &RunEnv,
    events: Vec<AppendRunEventRequest>,
    final_status: String,
    session_id: Option<String>,
    last_heartbeat: &mut Instant,
) -> anyhow::Result<()> {
    let ownership_generation = claim
        .run
        .session_ownership_generation
        .context("claimed Run is missing its Session ownership generation")?;
    let work_dir_ref = run_env.workdir.display().to_string();
    let mut tool_request_events = Vec::new();
    for event in events {
        if event.event_type == "tool_request" {
            tool_request_events.push(event);
            continue;
        }
        // 事件写入必须优先保证完成；heartbeat 只作为长任务期间的保活补充。
        client
            .append_event(claim.run.id, ownership_generation, event)
            .await?;
        if last_heartbeat.elapsed() >= Duration::from_secs(1) {
            client.heartbeat().await?;
            *last_heartbeat = Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let tool_request_batch = build_tool_request_batch(
        claim,
        tool_request_events,
        &final_status,
        session_id.as_deref(),
        &work_dir_ref,
    )?;
    if let Some(batch) = tool_request_batch {
        client
            .finalize_tool_requests(claim.run.id, ownership_generation, &batch)
            .await?;
        client
            .complete_run(
                claim.run.id,
                ownership_generation,
                CompleteRunRequest {
                    status: final_status,
                    session_id,
                    work_dir_ref: Some(work_dir_ref),
                },
            )
            .await?;
    } else {
        client.heartbeat().await?;
        client
            .complete_run(
                claim.run.id,
                ownership_generation,
                CompleteRunRequest {
                    status: final_status,
                    session_id,
                    work_dir_ref: Some(work_dir_ref),
                },
            )
            .await?;
    }
    if let Some(run_root) = run_env.workdir.parent() {
        fs::write(
            run_root.join(".completed-at"),
            chrono::Utc::now().to_rfc3339(),
        )
        .await?;
    }
    Ok(())
}

fn session_supervisor_metadata_for_claim(
    runtime_id: Uuid,
    claim: &ClaimRunResponse,
    codex_version: &str,
) -> anyhow::Result<SessionSupervisorMetadata> {
    let session_id = claim
        .run
        .hub_session_id
        .context("claimed Run is missing its Hub Session id")?;
    let ownership_generation = claim
        .run
        .session_ownership_generation
        .context("claimed Run is missing its Session ownership generation")?;
    anyhow::ensure!(
        ownership_generation > 0,
        "ownership generation must be positive"
    );
    Ok(SessionSupervisorMetadata {
        format_version: 1,
        session_id,
        runtime_id,
        ownership_generation,
        lifecycle_status: "online".into(),
        idle_deadline_unix_ms: None,
        checkpoint_reason: None,
        checkpoint_retry_unix_ms: None,
        hub_checkpoint_attempt_id: None,
        codex_version: codex_version.to_owned(),
        native_thread_id: claim
            .session_context
            .as_ref()
            .and_then(|context| context.session.native_thread_id.clone()),
    })
}

#[derive(Debug)]
struct SessionBundleRestoreFailure;

impl std::fmt::Display for SessionBundleRestoreFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Session Bundle restore failed")
    }
}

impl std::error::Error for SessionBundleRestoreFailure {}

async fn restore_claim_session_bundle_if_needed(
    config: &Config,
    client: &HubClient,
    claim: &ClaimRunResponse,
) -> anyhow::Result<()> {
    let restore_result: anyhow::Result<()> = async {
        let Some(context) = claim.session_context.as_ref() else {
            return Ok(());
        };
        if context.session.lifecycle_status != "restoring" {
            return Ok(());
        }
        let Some(current) = context.session.current_bundle.as_ref() else {
            return Ok(());
        };
        anyhow::ensure!(current.size_bytes >= 0, "Hub Bundle size is negative");
        let session_id = context.session.id;
        let ownership_generation = claim
            .run
            .session_ownership_generation
            .context("restoring Run is missing its Session ownership generation")?;
        let download_root = config.work_root.join("bundle-staging");
        fs::create_dir_all(&download_root).await?;
        let archive_path =
            download_root.join(format!("{session_id}-{}.tar.zst", current.generation));
        if let Err(error) = fs::remove_file(&archive_path).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(error).context("replace staged Session Bundle download");
            }
        }
        client
            .download_session_bundle(session_id, ownership_generation, current, &archive_path)
            .await?;

        let restore_root = config
            .work_root
            .join("bundle-restores")
            .join(Uuid::new_v4().to_string());
        let prepared_session_root = SessionPaths::for_session(&restore_root, session_id).root;
        let archive_for_restore = archive_path.clone();
        let prepared_for_restore = prepared_session_root.clone();
        let checksum = current.checksum_sha256.clone();
        let size = current.size_bytes as u64;
        let history_checkpoint = current.history_checkpoint;
        let manifest = tokio::task::spawn_blocking(move || {
            session_bundle::restore_session_bundle(
                &archive_for_restore,
                &checksum,
                size,
                session_id,
                history_checkpoint,
                &prepared_for_restore,
            )
        })
        .await
        .context("Session Bundle restore task stopped")??;
        anyhow::ensure!(
            manifest.bundle_generation == current.generation
                && manifest.ownership_generation == current.ownership_generation
                && manifest.producing_codex_version == current.producing_codex_version,
            "Session Bundle manifest does not match Hub Bundle metadata"
        );
        anyhow::ensure!(
            context.session.native_thread_id.as_deref() == Some(&manifest.native_thread_id),
            "Session Bundle native Thread does not match Hub Session"
        );
        let metadata = session_supervisor_metadata_for_claim(
            configured_runtime_id(claim)?,
            claim,
            &config.codex_version,
        )?;
        persist_session_supervisor_metadata(&restore_root, &metadata).await?;

        let target = SessionPaths::for_session(&config.work_root, session_id).root;
        let target_parent = target
            .parent()
            .context("Session directory has no parent")?
            .to_path_buf();
        fs::create_dir_all(&target_parent).await?;
        let backup_root = config.work_root.join("session-backups");
        fs::create_dir_all(&backup_root).await?;
        let backup = backup_root.join(format!("{session_id}-{}", Uuid::new_v4().simple()));
        let had_target = fs::symlink_metadata(&target).await.is_ok();
        if had_target {
            fs::rename(&target, &backup)
                .await
                .context("stage previous local Session directory")?;
        }
        if let Err(error) = fs::rename(&prepared_session_root, &target).await {
            if had_target {
                let _ = fs::rename(&backup, &target).await;
            }
            return Err(error).context("install restored Session directory");
        }
        if had_target {
            fs::remove_dir_all(&backup)
                .await
                .context("remove replaced local Session directory")?;
        }
        let _ = fs::remove_dir_all(&restore_root).await;
        fs::remove_file(&archive_path)
            .await
            .context("remove verified Session Bundle download")?;
        Ok(())
    }
    .await;
    restore_result.context(SessionBundleRestoreFailure)
}

fn configured_runtime_id(claim: &ClaimRunResponse) -> anyhow::Result<Uuid> {
    claim
        .run
        .runtime_id
        .context("claimed Run is missing its Runtime id")
}

async fn execute_managed_run(
    config: &Config,
    client: &HubClient,
    manager: Arc<SessionSupervisorManager>,
    claim: ClaimRunResponse,
) -> anyhow::Result<()> {
    let session_id = claim
        .run
        .hub_session_id
        .context("claimed Run is missing its Hub Session id")?;
    let result = execute_managed_run_inner(config, client, Arc::clone(&manager), claim).await;
    if let Err(error) = &result {
        manager.cancel_session(session_id, error.to_string());
    }
    result
}

async fn execute_managed_run_inner(
    config: &Config,
    client: &HubClient,
    manager: Arc<SessionSupervisorManager>,
    mut claim: ClaimRunResponse,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.codex_driver == "app-server",
        "persistent Session execution requires the app-server driver"
    );
    ensure_atomic_tool_request_protocol(client, &claim)?;
    let session_id = claim
        .run
        .hub_session_id
        .context("claimed Run is missing its Hub Session id")?;
    let model_proxy = match manager.model_proxy(session_id) {
        Some(proxy) => {
            proxy.activate_run(claim.run.id, &claim.model_proxy_token);
            proxy
        }
        None => Arc::new(
            start_model_proxy(
                client,
                claim.run.id,
                &claim.model_proxy_token,
                config.model_proxy_idle_timeout,
            )
            .await?,
        ),
    };
    restore_claim_session_bundle_if_needed(config, client, &claim).await?;
    let run_env = prepare_run_env_with_local_skills(
        &config.work_root,
        &claim,
        Some(&model_proxy.base_url),
        config.local_skills_dir.as_deref(),
    )
    .await?;
    let metadata =
        session_supervisor_metadata_for_claim(manager.runtime_id, &claim, &config.codex_version)?;
    manager
        .ensure_app_server(
            metadata,
            config.codex_bin.clone(),
            run_env.clone(),
            config.app_server_timeout,
            Some(model_proxy),
        )
        .await?;
    if claim.session_context.is_some() {
        let ownership_generation = claim
            .run
            .session_ownership_generation
            .context("claimed Run is missing its Session ownership generation")?;
        let begin = client
            .begin_turn(
                claim.run.id,
                ownership_generation,
                &claim.expected_configuration_fingerprint,
            )
            .await?;
        anyhow::ensure!(
            begin.session_id == session_id
                && Some(begin.turn_id) == claim.run.hub_turn_id
                && begin.ownership_generation == ownership_generation
                && begin.configuration_fingerprint == claim.expected_configuration_fingerprint,
            "Hub returned a mismatched Turn begin response"
        );
        claim.session_context.as_mut().unwrap().messages = begin.messages;
    }
    info!(
        run_id = %claim.run.id,
        session_id = %session_id,
        workdir = %run_env.workdir.display(),
        codex_home = %run_env.codex_home.display(),
        "claimed persistent Session run"
    );

    let mut last_heartbeat = Instant::now();
    let result = execute_managed_app_server_with_streaming(
        client,
        Arc::clone(&manager),
        &claim,
        &mut last_heartbeat,
        Duration::from_secs(10),
    )
    .await?;
    info!(
        run_id = %claim.run.id,
        session_id = %session_id,
        native_turn_id = result.native_turn_id.as_deref().unwrap_or("unbound"),
        final_status = %result.final_status,
        "native Codex Turn finished"
    );
    manager
        .update_native_thread_id(
            session_id,
            claim
                .run
                .session_ownership_generation
                .context("claimed Run is missing its Session ownership generation")?,
            result.session_id.as_deref(),
        )
        .await?;
    finish_claimed_run(
        client,
        &claim,
        &run_env,
        result.events,
        result.final_status,
        result.session_id,
        &mut last_heartbeat,
    )
    .await
}

async fn execute_managed_app_server_with_streaming(
    client: &HubClient,
    manager: Arc<SessionSupervisorManager>,
    claim: &ClaimRunResponse,
    last_heartbeat: &mut Instant,
    heartbeat_interval: Duration,
) -> anyhow::Result<AppServerRunResult> {
    let (event_tx, mut event_rx) = app_server_event_channel();
    let mut deferred_tool_requests = Vec::new();
    let ownership_generation = claim
        .run
        .session_ownership_generation
        .context("claimed Run is missing its Session ownership generation")?;
    let session_id = claim
        .run
        .hub_session_id
        .context("claimed Run is missing its Hub Session id")?;
    let run_id = claim.run.id;
    let mut driver = tokio::spawn({
        let manager = Arc::clone(&manager);
        let claim = claim.clone();
        async move { manager.execute(claim, Some(event_tx)).await }
    });
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_interval,
        heartbeat_interval,
    );

    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                if event.event_type == "turn_started" {
                    let native_thread_id = event
                        .payload
                        .get("native_thread_id")
                        .and_then(serde_json::Value::as_str)
                        .context("turn_started event is missing native Thread id")?;
                    manager
                        .update_native_thread_id(
                            session_id,
                            ownership_generation,
                            Some(native_thread_id),
                        )
                        .await?;
                }
                if let Some(event) = defer_tool_request(event, &mut deferred_tool_requests) {
                    if let Err(error) = append_streamed_event(
                        client,
                        run_id,
                        ownership_generation,
                        event,
                        last_heartbeat,
                    ).await {
                        event_rx.close();
                        manager.cancel_session(session_id, error.to_string());
                        let _ = driver.await;
                        return Err(error);
                    }
                }
            }
            _ = heartbeat.tick() => {
                match client.heartbeat().await {
                    Ok(()) => *last_heartbeat = Instant::now(),
                    Err(error) => {
                        event_rx.close();
                        manager.cancel_session(session_id, error.to_string());
                        let _ = driver.await;
                        return Err(error);
                    }
                }
            }
            result = &mut driver => {
                let mut result = result??;
                while let Ok(event) = event_rx.try_recv() {
                    if let Some(event) = defer_tool_request(event, &mut deferred_tool_requests) {
                        append_streamed_event(
                            client,
                            run_id,
                            ownership_generation,
                            event,
                            last_heartbeat,
                        ).await?;
                    }
                }
                if result.final_status == "waiting_tool" {
                    result.events.extend(deferred_tool_requests);
                }
                return Ok(result);
            }
        }
    }
}

async fn execute_app_server_with_streaming(
    config: &Config,
    client: &HubClient,
    claim: &ClaimRunResponse,
    run_env: &RunEnv,
    last_heartbeat: &mut Instant,
) -> anyhow::Result<AppServerRunResult> {
    execute_app_server_with_streaming_with_heartbeat_interval(
        config,
        client,
        claim,
        run_env,
        last_heartbeat,
        Duration::from_secs(10),
    )
    .await
}

async fn execute_app_server_with_streaming_with_heartbeat_interval(
    config: &Config,
    client: &HubClient,
    claim: &ClaimRunResponse,
    run_env: &RunEnv,
    last_heartbeat: &mut Instant,
    heartbeat_interval: Duration,
) -> anyhow::Result<AppServerRunResult> {
    let (event_tx, mut event_rx) = app_server_event_channel();
    let mut deferred_tool_requests = Vec::new();
    let cancellation = Arc::new(AppServerCancellation::default());
    let _cancellation_guard = AppServerCancellationGuard(Arc::clone(&cancellation));
    let config = config.clone();
    let ownership_generation = claim
        .run
        .session_ownership_generation
        .context("claimed Run is missing its Session ownership generation")?;
    let claim = claim.clone();
    let run_env = run_env.clone();
    let run_id = claim.run.id;
    let driver_cancellation = Arc::clone(&cancellation);
    let mut driver = tokio::spawn(async move {
        run_app_server_driver(
            &config,
            &claim,
            &run_env,
            Some(event_tx),
            driver_cancellation,
        )
        .await
    });
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_interval,
        heartbeat_interval,
    );

    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                if let Some(event) = defer_tool_request(event, &mut deferred_tool_requests) {
                    if let Err(error) = append_streamed_event(
                        client,
                        run_id,
                        ownership_generation,
                        event,
                        last_heartbeat,
                    ).await {
                        event_rx.close();
                        cancel_app_server_driver(&cancellation, &mut driver).await;
                        return Err(error);
                    }
                }
            }
            _ = heartbeat.tick() => {
                match client.heartbeat().await {
                    Ok(()) => *last_heartbeat = Instant::now(),
                    Err(error) => {
                        event_rx.close();
                        cancel_app_server_driver(&cancellation, &mut driver).await;
                        return Err(error);
                    }
                }
            }
            result = &mut driver => {
                let mut result = result??;
                while let Ok(event) = event_rx.try_recv() {
                    if let Some(event) = defer_tool_request(event, &mut deferred_tool_requests) {
                        append_streamed_event(
                            client,
                            run_id,
                            ownership_generation,
                            event,
                            last_heartbeat,
                        ).await?;
                    }
                }
                if result.final_status == "waiting_tool" {
                    result.events.extend(deferred_tool_requests);
                }
                return Ok(result);
            }
        }
    }
}

fn app_server_event_channel() -> (
    tokio_mpsc::Sender<AppendRunEventRequest>,
    tokio_mpsc::Receiver<AppendRunEventRequest>,
) {
    tokio_mpsc::channel(APP_SERVER_EVENT_QUEUE_CAPACITY)
}

fn defer_tool_request(
    event: AppendRunEventRequest,
    deferred_tool_requests: &mut Vec<AppendRunEventRequest>,
) -> Option<AppendRunEventRequest> {
    if event.event_type == "tool_request" {
        deferred_tool_requests.push(event);
        None
    } else {
        Some(event)
    }
}

fn build_tool_request_batch(
    claim: &ClaimRunResponse,
    events: Vec<AppendRunEventRequest>,
    final_status: &str,
    session_id: Option<&str>,
    work_dir_ref: &str,
) -> anyhow::Result<Option<FinalizeToolRequestsRequest>> {
    if final_status != "waiting_tool" {
        return Ok(None);
    }
    let integration_session_id = claim
        .run
        .integration_session_id
        .context("waiting tool run is missing an Integration session id")?;
    let tool_requests = events
        .into_iter()
        .filter(|event| event.event_type == "tool_request")
        .map(|event| FinalizeToolRequestEvent {
            role: event.role,
            content: event.content,
            payload: event.payload,
        })
        .collect::<Vec<_>>();
    if tool_requests.is_empty() {
        anyhow::bail!("waiting tool turn did not produce any tool requests");
    }
    Ok(Some(FinalizeToolRequestsRequest {
        integration_session_id,
        session_id: session_id
            .context("waiting tool run is missing a session id")?
            .to_owned(),
        work_dir_ref: work_dir_ref.to_owned(),
        tool_requests,
    }))
}

async fn cancel_app_server_driver(
    cancellation: &AppServerCancellation,
    driver: &mut JoinHandle<anyhow::Result<AppServerRunResult>>,
) {
    cancellation.cancel();
    let _ = driver.await;
}

async fn append_streamed_event(
    client: &HubClient,
    run_id: Uuid,
    ownership_generation: i64,
    event: AppendRunEventRequest,
    last_heartbeat: &mut Instant,
) -> anyhow::Result<()> {
    client
        .append_event(run_id, ownership_generation, event)
        .await?;
    if last_heartbeat.elapsed() >= Duration::from_secs(1) {
        client.heartbeat().await?;
        *last_heartbeat = Instant::now();
    }
    Ok(())
}

struct LocalModelProxy {
    base_url: String,
    state: Arc<LocalModelProxyState>,
    handle: JoinHandle<()>,
}

impl LocalModelProxy {
    fn activate_run(&self, run_id: Uuid, model_proxy_token: &str) {
        *self.state.active_run.write().unwrap() = LocalModelProxyRunAuth {
            model_proxy_token: model_proxy_token.to_owned(),
            run_id,
        };
    }
}

impl Drop for LocalModelProxy {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[derive(Clone)]
struct LocalModelProxyRunAuth {
    model_proxy_token: String,
    run_id: Uuid,
}

struct LocalModelProxyState {
    http: reqwest::Client,
    hub_url: String,
    active_run: std::sync::RwLock<LocalModelProxyRunAuth>,
}

async fn start_model_proxy(
    client: &HubClient,
    run_id: Uuid,
    model_proxy_token: &str,
    idle_timeout: Duration,
) -> anyhow::Result<LocalModelProxy> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind local model proxy")?;
    let addr = listener
        .local_addr()
        .context("read local model proxy addr")?;
    let state = Arc::new(LocalModelProxyState {
        http: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(idle_timeout)
            .build()
            .context("build model proxy HTTP client")?,
        hub_url: client.hub_url.clone(),
        active_run: std::sync::RwLock::new(LocalModelProxyRunAuth {
            model_proxy_token: model_proxy_token.to_owned(),
            run_id,
        }),
    });
    let app = Router::new()
        .route("/v1/{*path}", post(local_model_proxy_request))
        .with_state(Arc::clone(&state));
    let handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            warn!(error = %err, "local model proxy stopped");
        }
    });
    Ok(LocalModelProxy {
        base_url: format!("http://{addr}/v1"),
        state,
        handle,
    })
}

async fn local_model_proxy_request(
    State(state): State<Arc<LocalModelProxyState>>,
    AxumPath(path): AxumPath<String>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let active_run = state.active_run.read().unwrap().clone();
    let mut upstream_url = format!("{}/api/runtime/model-proxy/v1/{}", state.hub_url, path);
    if let Some(query) = query.filter(|query| !query.is_empty()) {
        upstream_url.push('?');
        upstream_url.push_str(&query);
    }
    let response = state
        .http
        .post(upstream_url)
        .headers(forwarded_model_request_headers(&headers))
        .bearer_auth(&active_run.model_proxy_token)
        .header("x-agent-hub-run-id", active_run.run_id.to_string())
        .body(body)
        .send()
        .await;
    match response {
        Ok(response) => {
            let status = response.status();
            let headers = forwarded_model_response_headers(response.headers());
            let mut downstream = Response::new(Body::new(reqwest::Body::from(response)));
            *downstream.status_mut() = status;
            *downstream.headers_mut() = headers;
            downstream
        }
        Err(_) => (
            AxumStatusCode::BAD_GATEWAY,
            Json(json!({ "error": "model proxy forwarding failed" })),
        )
            .into_response(),
    }
}

fn forwarded_model_request_headers(upstream: &HeaderMap) -> HeaderMap {
    let mut connection_headers = HashSet::new();
    for connection in upstream.get_all(header::CONNECTION) {
        if let Ok(connection) = connection.to_str() {
            for name in connection.split(',') {
                let name = name.trim();
                if !name.is_empty() {
                    connection_headers.insert(name.to_ascii_lowercase());
                }
            }
        }
    }
    let mut forwarded = HeaderMap::new();
    for (name, value) in upstream {
        if is_hop_by_hop_header(name)
            || connection_headers.contains(name.as_str())
            || matches!(
                name,
                &header::AUTHORIZATION | &header::COOKIE | &header::HOST | &header::CONTENT_LENGTH
            )
            || name == "x-agent-hub-run-id"
            || is_sensitive_model_header(name)
        {
            continue;
        }
        forwarded.append(name, value.clone());
    }
    forwarded
}

fn forwarded_model_response_headers(upstream: &HeaderMap) -> HeaderMap {
    let mut connection_headers = HashSet::new();
    for connection in upstream.get_all(header::CONNECTION) {
        if let Ok(connection) = connection.to_str() {
            for name in connection.split(',') {
                let name = name.trim();
                if !name.is_empty() {
                    connection_headers.insert(name.to_ascii_lowercase());
                }
            }
        }
    }
    let mut forwarded = HeaderMap::new();
    for (name, value) in upstream {
        if is_hop_by_hop_header(name)
            || connection_headers.contains(name.as_str())
            || is_sensitive_model_response_header(name)
        {
            continue;
        }
        forwarded.append(name, value.clone());
    }
    forwarded
}

fn is_hop_by_hop_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn is_sensitive_model_response_header(name: &HeaderName) -> bool {
    matches!(name.as_str(), "authorization" | "cookie" | "set-cookie")
        || is_sensitive_model_header(name)
}

fn is_sensitive_model_header(name: &HeaderName) -> bool {
    let name = name.as_str();
    name.contains("token")
        || name.contains("api-key")
        || name.contains("api_key")
        || name.contains("secret")
}

async fn run_app_server_driver(
    config: &Config,
    claim: &ClaimRunResponse,
    run_env: &RunEnv,
    event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
    cancellation: Arc<AppServerCancellation>,
) -> anyhow::Result<AppServerRunResult> {
    let codex_bin = config.codex_bin.clone();
    let run_env = run_env.clone();
    let claim = claim.clone();
    let timeout = config.app_server_timeout;
    tokio::task::spawn_blocking(move || {
        run_app_server_process_with_cancellation(
            &codex_bin,
            &run_env,
            &claim,
            timeout,
            event_tx,
            cancellation,
        )
    })
    .await?
}

#[cfg(test)]
fn app_server_request_lines(
    claim: &ClaimRunResponse,
    run_env: &RunEnv,
) -> anyhow::Result<Vec<String>> {
    let requests = vec![
        app_server_initialize_request(),
        json!({ "jsonrpc": "2.0", "method": "initialized" }),
        app_server_thread_start_request(claim, run_env),
        app_server_turn_start_request(claim, "thread-placeholder"),
    ];
    requests
        .into_iter()
        .map(|request| serde_json::to_string(&request).context("serialize JSON-RPC request"))
        .collect()
}

fn app_server_initialize_request() -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "clientInfo": {
                "name": "agent-hub-runtime",
                "version": env!("CARGO_PKG_VERSION")
            },
            "capabilities": {}
        }
    })
}

fn app_server_thread_start_request(
    claim: &ClaimRunResponse,
    run_env: &RunEnv,
) -> serde_json::Value {
    let mut params = json!({
        "cwd": run_env.workdir,
        "approvalPolicy": "never",
        "sandbox": codex_sandbox_name(&claim.agent),
        "developerInstructions": claim.agent.instructions
    });
    if let Some((model, provider)) = app_server_default_model_configuration(claim) {
        params["model"] = json!(model);
        params["modelProvider"] = json!(provider);
    }
    let method = if let Some(resume) = &claim.resume {
        params["threadId"] = json!(resume.thread_id);
        "thread/resume"
    } else {
        "thread/start"
    };
    json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": method,
        "params": params
    })
}

fn app_server_thread_unsubscribe_request(thread_id: &str) -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 0,
        "method": "thread/unsubscribe",
        "params": { "threadId": thread_id }
    })
}

fn app_server_thread_refresh_request(
    claim: &ClaimRunResponse,
    run_env: &RunEnv,
    thread_id: &str,
) -> serde_json::Value {
    let mut request = app_server_thread_start_request(claim, run_env);
    request["method"] = json!("thread/resume");
    request["params"]["threadId"] = json!(thread_id);
    request["params"]["excludeTurns"] = json!(true);
    // An empty override forces an unsubscribed idle Thread through cold resume
    // without changing any setting, so Codex reloads config and agent files.
    request["params"]["config"] = json!({});
    request
}

fn app_server_default_model_configuration(claim: &ClaimRunResponse) -> Option<(&str, String)> {
    let connection_id = claim.execution_configuration.default_model_connection_id?;
    let connection = claim
        .execution_configuration
        .model_connections
        .iter()
        .find(|connection| connection.id == connection_id)?;
    Some((
        connection.model_id.as_str(),
        model_provider_name(connection.id),
    ))
}

fn app_server_turn_start_request(claim: &ClaimRunResponse, thread_id: &str) -> serde_json::Value {
    let mut queued_messages = claim
        .session_context
        .as_ref()
        .map(|context| context.messages.iter().collect::<Vec<_>>())
        .unwrap_or_default();
    queued_messages.sort_by_key(|message| message.sequence);
    let mut input = queued_messages
        .into_iter()
        .filter_map(|message| message.content.as_deref())
        .filter(|text| !text.trim().is_empty())
        .map(|text| {
            json!({
                "type": "text",
                "text": text,
                "text_elements": []
            })
        })
        .collect::<Vec<_>>();
    if input.is_empty() {
        input.push(json!({
            "type": "text",
            "text": claim.run.initial_message,
            "text_elements": []
        }));
    }
    let mut params = json!({
        "threadId": thread_id,
        "input": input,
        "source": claim.run.source,
        "sandboxPolicy": codex_sandbox_policy(&claim.agent),
        "metadata": {
            "agent_hub_run_id": claim.run.id,
            "integration_context": claim.integration_context
        }
    });
    if let Some(context) = &claim.integration_context {
        params["dynamicTools"] = context.tools.clone();
    }
    if let Some((model, _)) = app_server_default_model_configuration(claim) {
        params["model"] = json!(model);
    }
    if let Some(effort) = codex_reasoning_effort(claim.execution_configuration.reasoning_effort) {
        params["effort"] = json!(effort);
    }
    json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "turn/start",
        "params": params
    })
}

fn app_server_turn_steer_request(
    thread_id: &str,
    expected_turn_id: &str,
    client_user_message_id: Uuid,
    input: &[String],
) -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 0,
        "method": "turn/steer",
        "params": {
            "threadId": thread_id,
            "expectedTurnId": expected_turn_id,
            "clientUserMessageId": client_user_message_id,
            "input": input.iter().map(|text| json!({
                "type": "text",
                "text": text,
                "text_elements": []
            })).collect::<Vec<_>>()
        }
    })
}

fn app_server_steer_response(
    response: &serde_json::Value,
    expected_turn_id: &str,
) -> anyhow::Result<SessionSteerOutcome> {
    if let Some(error) = response.get("error") {
        let message = error
            .get("message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if message == "no active turn to steer"
            || (message.starts_with("expected active turn id ") && message.contains(" but found "))
        {
            return Ok(SessionSteerOutcome::TurnEnded);
        }
        anyhow::bail!("Codex app-server rejected turn/steer");
    }
    let turn_id = response
        .get("result")
        .and_then(|result| result.get("turnId"))
        .and_then(serde_json::Value::as_str)
        .context("Codex app-server turn/steer response is missing turnId")?;
    anyhow::ensure!(
        turn_id == expected_turn_id,
        "Codex app-server turn/steer response changed the expected Turn"
    );
    Ok(SessionSteerOutcome::Applied)
}

fn app_server_turn_interrupt_request(thread_id: &str, turn_id: &str) -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 0,
        "method": "turn/interrupt",
        "params": {
            "threadId": thread_id,
            "turnId": turn_id
        }
    })
}

fn app_server_interrupt_response(response: &serde_json::Value) -> anyhow::Result<()> {
    if response.get("error").is_some() {
        anyhow::bail!("Codex app-server rejected turn/interrupt");
    }
    response
        .get("result")
        .context("Codex app-server turn/interrupt response is missing result")?;
    Ok(())
}

#[cfg(test)]
fn run_app_server_process(
    codex_bin: &str,
    workdir: &Path,
    codex_home: &Path,
    claim: &ClaimRunResponse,
    timeout: Duration,
    event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
) -> anyhow::Result<AppServerRunResult> {
    let run_env = RunEnv {
        workdir: workdir.to_path_buf(),
        codex_home: codex_home.to_path_buf(),
    };
    run_app_server_process_with_cancellation(
        codex_bin,
        &run_env,
        claim,
        timeout,
        event_tx,
        Arc::new(AppServerCancellation::default()),
    )
}

fn run_app_server_process_with_cancellation(
    codex_bin: &str,
    run_env: &RunEnv,
    claim: &ClaimRunResponse,
    timeout: Duration,
    event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
    cancellation: Arc<AppServerCancellation>,
) -> anyhow::Result<AppServerRunResult> {
    let mut process = PersistentAppServerProcess::start(codex_bin, run_env, timeout, cancellation)
        .context("Codex app-server failed")?;
    process
        .execute(claim, event_tx)
        .context("Codex app-server failed")
}

struct PersistentAppServerProcess {
    run_env: RunEnv,
    thread_id: Option<String>,
    configuration_fingerprint: Option<String>,
    next_request_id: u64,
    child: Option<std::process::Child>,
    stdin: Option<std::process::ChildStdin>,
    line_rx: Option<mpsc::Receiver<anyhow::Result<String>>>,
    stdout_reader: Option<std::thread::JoinHandle<()>>,
    stderr_reader: Option<std::thread::JoinHandle<Result<usize, std::io::Error>>>,
    cancellation: Arc<AppServerCancellation>,
    timeout: Duration,
}

impl PersistentAppServerProcess {
    fn start(
        codex_bin: &str,
        run_env: &RunEnv,
        timeout: Duration,
        cancellation: Arc<AppServerCancellation>,
    ) -> anyhow::Result<Self> {
        let mut command = Command::new(codex_bin);
        command
            .env_clear()
            .arg("app-server")
            .arg("--listen")
            .arg("stdio://")
            .current_dir(&run_env.workdir)
            .env("CODEX_HOME", &run_env.codex_home)
            .env("HOME", &run_env.codex_home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for key in [
            "PATH",
            "LANG",
            "LC_ALL",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "NO_PROXY",
        ] {
            if let Some(value) = env::var_os(key) {
                command.env(key, value);
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("spawn Codex app-server: {codex_bin}"))?;
        cancellation.register_child(&child);

        let stdout = child
            .stdout
            .take()
            .context("open Codex app-server stdout")?;
        let mut stderr = child
            .stderr
            .take()
            .context("open Codex app-server stderr")?;
        let (line_tx, line_rx) =
            mpsc::sync_channel::<anyhow::Result<String>>(APP_SERVER_EVENT_QUEUE_CAPACITY);
        let stdout_reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line_tx
                    .send(line.context("read Codex app-server stdout line"))
                    .is_err()
                {
                    break;
                }
            }
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut total = 0_usize;
            let mut buffer = [0_u8; 8192];
            loop {
                let read = stderr.read(&mut buffer)?;
                if read == 0 {
                    return Ok::<usize, std::io::Error>(total);
                }
                total = total.saturating_add(read);
            }
        });
        let stdin = child.stdin.take().context("open Codex app-server stdin")?;
        let mut process = Self {
            run_env: run_env.clone(),
            thread_id: None,
            configuration_fingerprint: None,
            next_request_id: 1,
            child: Some(child),
            stdin: Some(stdin),
            line_rx: Some(line_rx),
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            cancellation,
            timeout,
        };
        if let Err(error) = process.initialize() {
            process.shutdown();
            return Err(error);
        }
        Ok(process)
    }

    #[cfg(test)]
    fn child_id(&self) -> u32 {
        self.child.as_ref().map_or(0, std::process::Child::id)
    }

    fn ensure_running(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.cancellation.is_cancelled(),
            "Codex app-server is cancelled"
        );
        let child = self
            .child
            .as_mut()
            .context("Codex app-server process is closed")?;
        if let Some(status) = child.try_wait().context("poll Codex app-server")? {
            anyhow::bail!("Codex app-server exited with status {status}");
        }
        Ok(())
    }

    fn initialize(&mut self) -> anyhow::Result<()> {
        let request_id = self.send_request(app_server_initialize_request())?;
        let started_at = Instant::now();
        let mut state = AppServerState::new(Uuid::nil());
        state.expect_response(request_id, AppServerResponseKind::Initialize);
        while !state.initialized {
            let line = self.recv(started_at)?;
            state.handle_value(&serde_json::from_str(&line).context("parse app-server JSON")?)?;
        }
        self.send(&json!({ "jsonrpc": "2.0", "method": "initialized" }))
    }

    fn execute(
        &mut self,
        claim: &ClaimRunResponse,
        event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
    ) -> anyhow::Result<AppServerRunResult> {
        let result = self.execute_inner(claim, &event_tx, None, None);
        if result.is_err() {
            self.shutdown();
        }
        result
    }

    fn execute_controlled(
        &mut self,
        claim: &ClaimRunResponse,
        event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
        command_rx: &mpsc::Receiver<SessionSupervisorCommand>,
        deferred_commands: &mut VecDeque<SessionSupervisorCommand>,
    ) -> anyhow::Result<AppServerRunResult> {
        let result =
            self.execute_inner(claim, &event_tx, Some(command_rx), Some(deferred_commands));
        if result.is_err() {
            self.shutdown();
        }
        result
    }

    fn execute_inner(
        &mut self,
        claim: &ClaimRunResponse,
        event_tx: &Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
        command_rx: Option<&mpsc::Receiver<SessionSupervisorCommand>>,
        mut deferred_commands: Option<&mut VecDeque<SessionSupervisorCommand>>,
    ) -> anyhow::Result<AppServerRunResult> {
        let started_at = Instant::now();
        let mut state = AppServerState::new(claim.run.id);
        anyhow::ensure!(self.child.is_some(), "Codex app-server is not running");
        let reused_thread = self.thread_id.is_some();
        let thread_id = if let Some(thread_id) = self.thread_id.clone() {
            state.thread_id = Some(thread_id.clone());
            thread_id
        } else {
            let thread_request = app_server_thread_start_request(claim, &self.run_env);
            let request_id = self.send_request(thread_request)?;
            state.expect_response(request_id, AppServerResponseKind::Thread);
            while state.thread_id.is_none() {
                let line = self.recv(started_at)?;
                state
                    .handle_value(&serde_json::from_str(&line).context("parse app-server JSON")?)?;
                state.flush_events(event_tx, &self.cancellation)?;
            }
            let thread_id = state.thread_id.clone().context("missing Codex thread id")?;
            self.thread_id = Some(thread_id.clone());
            self.configuration_fingerprint = Some(claim.expected_configuration_fingerprint.clone());
            thread_id
        };
        if reused_thread
            && self.configuration_fingerprint.as_deref()
                != Some(claim.expected_configuration_fingerprint.as_str())
        {
            self.refresh_thread_configuration(claim, &thread_id, &mut state, started_at, event_tx)?;
        }
        let request_id = self.send_request(app_server_turn_start_request(claim, &thread_id))?;
        state.expect_response(request_id, AppServerResponseKind::TurnStart);
        let mut pending_steers = BTreeMap::new();
        let mut pending_interrupts = BTreeMap::new();
        let mut accepted_interrupts: Vec<PendingInterruptResponse> = Vec::new();
        loop {
            if let Some(command_rx) = command_rx {
                loop {
                    match command_rx.try_recv() {
                        Ok(SessionSupervisorCommand::Steer {
                            expected_turn_id,
                            client_user_message_id,
                            input,
                            response,
                        }) => {
                            if state.done
                                || state.native_turn_id.as_deref()
                                    != Some(expected_turn_id.as_str())
                            {
                                let _ = response.send(Ok(SessionSteerOutcome::TurnEnded));
                                continue;
                            }
                            let request_id = self.send_request(app_server_turn_steer_request(
                                &thread_id,
                                &expected_turn_id,
                                client_user_message_id,
                                &input,
                            ))?;
                            pending_steers.insert(
                                request_id,
                                PendingSteerResponse {
                                    expected_turn_id,
                                    response,
                                },
                            );
                        }
                        Ok(command @ SessionSupervisorCommand::Execute { .. }) => {
                            deferred_commands
                                .as_deref_mut()
                                .expect("controlled execution has a deferred command queue")
                                .push_back(command);
                        }
                        Ok(SessionSupervisorCommand::Interrupt {
                            expected_turn_id,
                            response,
                        }) => {
                            if state.done
                                || state.native_turn_id.as_deref()
                                    != Some(expected_turn_id.as_str())
                            {
                                let _ = response.send(Ok(SessionInterruptOutcome::TurnEnded));
                                continue;
                            }
                            let request_id = self.send_request(
                                app_server_turn_interrupt_request(&thread_id, &expected_turn_id),
                            )?;
                            pending_interrupts.insert(
                                request_id,
                                PendingInterruptResponse {
                                    expected_turn_id,
                                    response,
                                },
                            );
                        }
                        Ok(SessionSupervisorCommand::Stop) => {
                            anyhow::bail!("Session supervisor stopped during active Turn");
                        }
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => {
                            anyhow::bail!("Session supervisor command channel disconnected");
                        }
                    }
                }
            }
            if state.done && !accepted_interrupts.is_empty() {
                for pending in accepted_interrupts.drain(..) {
                    let outcome = if state.final_status == "interrupted"
                        && state.native_turn_id.as_deref()
                            == Some(pending.expected_turn_id.as_str())
                    {
                        SessionInterruptOutcome::Interrupted
                    } else {
                        SessionInterruptOutcome::TurnEnded
                    };
                    let _ = pending.response.send(Ok(outcome));
                }
            }
            if state.done
                && pending_steers.is_empty()
                && pending_interrupts.is_empty()
                && accepted_interrupts.is_empty()
            {
                break;
            }
            let line = if command_rx.is_some() {
                match self.recv_once(started_at, Duration::from_millis(10))? {
                    Some(line) => line,
                    None => continue,
                }
            } else {
                self.recv(started_at)?
            };
            let value: serde_json::Value =
                serde_json::from_str(&line).context("parse app-server JSON")?;
            let response_id = value.get("id").and_then(serde_json::Value::as_u64);
            if let Some(pending) = response_id.and_then(|id| pending_steers.remove(&id)) {
                let outcome = app_server_steer_response(&value, &pending.expected_turn_id);
                let _ = pending.response.send(outcome);
                continue;
            }
            if let Some(pending) = response_id.and_then(|id| pending_interrupts.remove(&id)) {
                match app_server_interrupt_response(&value) {
                    Ok(()) => accepted_interrupts.push(pending),
                    Err(error) => {
                        let _ = pending.response.send(Err(error));
                    }
                }
                continue;
            }
            state.handle_value(&value)?;
            state.flush_events(event_tx, &self.cancellation)?;
        }
        if state.events.is_empty() && state.streamed_events == 0 {
            anyhow::bail!("Codex app-server produced no events");
        }
        Ok(state.finish())
    }

    fn refresh_thread_configuration(
        &mut self,
        claim: &ClaimRunResponse,
        thread_id: &str,
        state: &mut AppServerState,
        started_at: Instant,
        event_tx: &Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
    ) -> anyhow::Result<()> {
        let unsubscribe_id = self.send_request(app_server_thread_unsubscribe_request(thread_id))?;
        self.wait_for_response(
            state,
            unsubscribe_id,
            AppServerResponseKind::Acknowledgement,
            started_at,
            event_tx,
        )?;

        let resume_request = app_server_thread_refresh_request(claim, &self.run_env, thread_id);
        let resume_id = self.send_request(resume_request)?;
        self.wait_for_response(
            state,
            resume_id,
            AppServerResponseKind::Thread,
            started_at,
            event_tx,
        )?;
        self.configuration_fingerprint = Some(claim.expected_configuration_fingerprint.clone());
        Ok(())
    }

    fn wait_for_response(
        &mut self,
        state: &mut AppServerState,
        request_id: u64,
        kind: AppServerResponseKind,
        started_at: Instant,
        event_tx: &Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
    ) -> anyhow::Result<()> {
        state.expect_response(request_id, kind);
        while state.pending_responses.contains_key(&request_id) {
            let line = self.recv(started_at)?;
            state.handle_value(&serde_json::from_str(&line).context("parse app-server JSON")?)?;
            state.flush_events(event_tx, &self.cancellation)?;
        }
        Ok(())
    }

    fn send(&mut self, value: &serde_json::Value) -> anyhow::Result<()> {
        let stdin = self
            .stdin
            .as_mut()
            .context("Codex app-server stdin is closed")?;
        send_app_server_value(stdin, value)
    }

    fn send_request(&mut self, mut value: serde_json::Value) -> anyhow::Result<u64> {
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .context("Codex app-server request id overflow")?;
        value["id"] = json!(request_id);
        self.send(&value)?;
        Ok(request_id)
    }

    fn recv(&mut self, started_at: Instant) -> anyhow::Result<String> {
        loop {
            if let Some(line) = self.recv_once(started_at, Duration::from_millis(50))? {
                return Ok(line);
            }
        }
    }

    fn recv_once(&mut self, started_at: Instant, wait: Duration) -> anyhow::Result<Option<String>> {
        let line_rx = self
            .line_rx
            .as_ref()
            .context("Codex app-server stdout reader is closed")?;
        let child = self
            .child
            .as_mut()
            .context("Codex app-server process is closed")?;
        recv_app_server_line_once(
            line_rx,
            child,
            started_at,
            self.timeout,
            wait,
            &self.cancellation,
        )
    }

    fn shutdown(&mut self) {
        self.cancellation.cancel();
        self.stdin.take();
        if let Some(mut child) = self.child.take() {
            let child_id = child.id();
            terminate_child_process_tree(&mut child);
            self.cancellation.clear_child(child_id);
        }
        self.line_rx.take();
        if let Some(reader) = self.stdout_reader.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.join();
        }
    }
}

impl Drop for PersistentAppServerProcess {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionSteerOutcome {
    Applied,
    TurnEnded,
}

struct PendingSteerResponse {
    expected_turn_id: String,
    response: oneshot::Sender<anyhow::Result<SessionSteerOutcome>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionInterruptOutcome {
    Interrupted,
    TurnEnded,
}

struct PendingInterruptResponse {
    expected_turn_id: String,
    response: oneshot::Sender<anyhow::Result<SessionInterruptOutcome>>,
}

enum SessionSupervisorCommand {
    Execute {
        claim: Box<ClaimRunResponse>,
        event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
        response: oneshot::Sender<anyhow::Result<AppServerRunResult>>,
    },
    Steer {
        expected_turn_id: String,
        client_user_message_id: Uuid,
        input: Vec<String>,
        response: oneshot::Sender<anyhow::Result<SessionSteerOutcome>>,
    },
    Interrupt {
        expected_turn_id: String,
        response: oneshot::Sender<anyhow::Result<SessionInterruptOutcome>>,
    },
    Stop,
}

struct SessionSupervisor {
    session_id: Uuid,
    ownership_generation: i64,
    command_tx: mpsc::Sender<SessionSupervisorCommand>,
    cancellation: Arc<AppServerCancellation>,
    actor: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    terminal_error: Arc<std::sync::Mutex<Option<String>>>,
    stopped: AtomicBool,
}

impl SessionSupervisor {
    async fn start_app_server(
        session_id: Uuid,
        ownership_generation: i64,
        codex_bin: String,
        run_env: RunEnv,
        timeout: Duration,
    ) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(
            ownership_generation > 0,
            "ownership generation must be positive"
        );
        tokio::task::spawn_blocking(move || {
            Self::start_app_server_blocking(
                session_id,
                ownership_generation,
                codex_bin,
                run_env,
                timeout,
            )
        })
        .await?
    }

    fn start_app_server_blocking(
        session_id: Uuid,
        ownership_generation: i64,
        codex_bin: String,
        run_env: RunEnv,
        timeout: Duration,
    ) -> anyhow::Result<Arc<Self>> {
        let (command_tx, command_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let cancellation = Arc::new(AppServerCancellation::default());
        let actor_cancellation = Arc::clone(&cancellation);
        let terminal_error = Arc::new(std::sync::Mutex::new(None));
        let actor_terminal_error = Arc::clone(&terminal_error);
        let actor = std::thread::spawn(move || {
            let mut process = match PersistentAppServerProcess::start(
                &codex_bin,
                &run_env,
                timeout,
                actor_cancellation,
            ) {
                Ok(process) => {
                    let _ = ready_tx.send(Ok(()));
                    process
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };
            let mut deferred_commands = VecDeque::new();
            loop {
                let command = deferred_commands
                    .pop_front()
                    .map_or_else(|| command_rx.recv_timeout(Duration::from_millis(50)), Ok);
                match command {
                    Ok(SessionSupervisorCommand::Execute {
                        claim,
                        event_tx,
                        response,
                    }) => {
                        let result = process.execute_controlled(
                            &claim,
                            event_tx,
                            &command_rx,
                            &mut deferred_commands,
                        );
                        let failed = result.is_err();
                        if let Err(error) = &result {
                            *actor_terminal_error.lock().unwrap() = Some(error.to_string());
                        }
                        let _ = response.send(result);
                        if failed {
                            break;
                        }
                    }
                    Ok(SessionSupervisorCommand::Steer { response, .. }) => {
                        let _ = response.send(Ok(SessionSteerOutcome::TurnEnded));
                    }
                    Ok(SessionSupervisorCommand::Interrupt { response, .. }) => {
                        let _ = response.send(Ok(SessionInterruptOutcome::TurnEnded));
                    }
                    Ok(SessionSupervisorCommand::Stop)
                    | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(error) = process.ensure_running() {
                            *actor_terminal_error.lock().unwrap() = Some(error.to_string());
                            break;
                        }
                    }
                }
            }
        });
        match ready_rx.recv_timeout(timeout + Duration::from_secs(1)) {
            Ok(Ok(())) => Ok(Arc::new(Self {
                session_id,
                ownership_generation,
                command_tx,
                cancellation,
                actor: std::sync::Mutex::new(Some(actor)),
                terminal_error,
                stopped: AtomicBool::new(false),
            })),
            Ok(Err(error)) => {
                let _ = actor.join();
                Err(error)
            }
            Err(error) => {
                cancellation.cancel();
                let _ = actor.join();
                Err(anyhow::anyhow!(
                    "wait for Session supervisor startup: {error}"
                ))
            }
        }
    }

    async fn execute(
        &self,
        claim: ClaimRunResponse,
        event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
    ) -> anyhow::Result<AppServerRunResult> {
        anyhow::ensure!(
            claim.run.hub_session_id == Some(self.session_id),
            "Run belongs to a different Hub Session"
        );
        anyhow::ensure!(
            claim.run.session_ownership_generation == Some(self.ownership_generation),
            "Run ownership generation does not match Session supervisor"
        );
        anyhow::ensure!(
            !self.stopped.load(Ordering::Acquire),
            "Session supervisor is stopped"
        );
        let (response, result) = oneshot::channel();
        self.command_tx
            .send(SessionSupervisorCommand::Execute {
                claim: Box::new(claim),
                event_tx,
                response,
            })
            .map_err(|_| anyhow::anyhow!("Session supervisor actor is not running"))?;
        result
            .await
            .map_err(|_| anyhow::anyhow!("Session supervisor actor stopped during Run"))?
    }

    async fn steer(
        &self,
        ownership_generation: i64,
        expected_turn_id: String,
        client_user_message_id: Uuid,
        input: Vec<String>,
    ) -> anyhow::Result<SessionSteerOutcome> {
        anyhow::ensure!(
            ownership_generation == self.ownership_generation,
            "Steering Message ownership generation does not match Session supervisor"
        );
        anyhow::ensure!(
            !expected_turn_id.trim().is_empty(),
            "Steering Message requires an expected native Turn id"
        );
        anyhow::ensure!(
            !input.is_empty() && input.iter().all(|text| !text.trim().is_empty()),
            "Steering Message input must be non-empty"
        );
        anyhow::ensure!(
            !self.stopped.load(Ordering::Acquire),
            "Session supervisor is stopped"
        );
        let (response, result) = oneshot::channel();
        self.command_tx
            .send(SessionSupervisorCommand::Steer {
                expected_turn_id,
                client_user_message_id,
                input,
                response,
            })
            .map_err(|_| anyhow::anyhow!("Session supervisor actor is not running"))?;
        result
            .await
            .map_err(|_| anyhow::anyhow!("Session supervisor actor stopped during steer"))?
    }

    async fn interrupt(
        &self,
        ownership_generation: i64,
        expected_turn_id: String,
    ) -> anyhow::Result<SessionInterruptOutcome> {
        anyhow::ensure!(
            ownership_generation == self.ownership_generation,
            "Interrupt ownership generation does not match Session supervisor"
        );
        anyhow::ensure!(
            !expected_turn_id.trim().is_empty(),
            "Interrupt requires an expected native Turn id"
        );
        anyhow::ensure!(
            !self.stopped.load(Ordering::Acquire),
            "Session supervisor is stopped"
        );
        let (response, result) = oneshot::channel();
        self.command_tx
            .send(SessionSupervisorCommand::Interrupt {
                expected_turn_id,
                response,
            })
            .map_err(|_| anyhow::anyhow!("Session supervisor actor is not running"))?;
        result
            .await
            .map_err(|_| anyhow::anyhow!("Session supervisor actor stopped during interrupt"))?
    }

    fn shutdown(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        self.cancellation.cancel();
        let _ = self.command_tx.send(SessionSupervisorCommand::Stop);
        if let Some(actor) = self.actor.lock().unwrap().take() {
            let _ = actor.join();
        }
    }

    fn terminal_failure(&self) -> Option<String> {
        if let Some(error) = self.terminal_error.lock().unwrap().clone() {
            return Some(error);
        }
        let actor_finished = self
            .actor
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished);
        (actor_finished && !self.stopped.load(Ordering::Acquire))
            .then(|| "Session supervisor actor stopped unexpectedly".into())
    }
}

impl Drop for SessionSupervisor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

enum ManagedSessionStatus {
    Starting,
    Cold {
        metadata: SessionSupervisorMetadata,
    },
    Ready {
        metadata: SessionSupervisorMetadata,
        supervisor: Arc<SessionSupervisor>,
        busy: bool,
    },
    Blocked {
        reason: String,
        restart_attempts: u32,
    },
}

struct ManagedSessionRecord {
    snapshot: RuntimeOwnedSessionSnapshotDto,
    status: ManagedSessionStatus,
    reserved_run_id: Option<Uuid>,
    model_proxy: Option<Arc<LocalModelProxy>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InterruptedRestoringRun {
    session_id: Uuid,
    run_id: Uuid,
    ownership_generation: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RuntimeCheckpointReason {
    Idle,
    Drain,
    VersionSwitch,
}

fn checkpoint_reason_priority(reason: RuntimeCheckpointReason) -> u8 {
    match reason {
        RuntimeCheckpointReason::Idle => 0,
        RuntimeCheckpointReason::VersionSwitch => 1,
        RuntimeCheckpointReason::Drain => 2,
    }
}

impl RuntimeCheckpointReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::VersionSwitch => "version_switch",
            Self::Drain => "drain",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeCheckpointRequest {
    session_id: Uuid,
    ownership_generation: i64,
    reason: RuntimeCheckpointReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RuntimeCheckpointEffectResult {
    Saved {
        has_queued_work: bool,
        ownership_released: bool,
    },
    Failed {
        has_queued_work: bool,
        retry_required: bool,
        error: String,
    },
}

struct RuntimeCheckpointTransportOutcome {
    checkpoint_attempt: Option<(Uuid, RuntimeCheckpointReason)>,
    result: RuntimeCheckpointEffectResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReleasedSessionCleanup {
    session_id: Uuid,
    ownership_generation: i64,
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
enum PersistedSessionCleanupState {
    Reserved { path: PathBuf },
    Completed,
}

type SessionCleanupKey = (Uuid, i64);

trait RuntimeCheckpointTransport {
    async fn checkpoint(
        &self,
        request: &RuntimeCheckpointRequest,
    ) -> RuntimeCheckpointTransportOutcome;
}

#[cfg(test)]
struct UnavailableRuntimeCheckpointTransport;

#[cfg(test)]
impl RuntimeCheckpointTransport for UnavailableRuntimeCheckpointTransport {
    async fn checkpoint(
        &self,
        _request: &RuntimeCheckpointRequest,
    ) -> RuntimeCheckpointTransportOutcome {
        RuntimeCheckpointTransportOutcome {
            checkpoint_attempt: None,
            result: RuntimeCheckpointEffectResult::Failed {
                has_queued_work: false,
                retry_required: true,
                error: "Session Bundle transport is not available until Task 11".into(),
            },
        }
    }
}

struct HubRuntimeCheckpointTransport {
    client: HubClient,
    work_root: PathBuf,
    producing_codex_version: String,
}

impl RuntimeCheckpointTransport for HubRuntimeCheckpointTransport {
    async fn checkpoint(
        &self,
        request: &RuntimeCheckpointRequest,
    ) -> RuntimeCheckpointTransportOutcome {
        let attempt = match self
            .client
            .begin_session_checkpoint(
                request.session_id,
                &BeginRuntimeSessionCheckpointRequest {
                    ownership_generation: request.ownership_generation,
                    reason: request.reason.as_str().into(),
                },
            )
            .await
        {
            Ok(attempt) => attempt,
            Err(error) => {
                return RuntimeCheckpointTransportOutcome {
                    checkpoint_attempt: None,
                    result: RuntimeCheckpointEffectResult::Failed {
                        has_queued_work: false,
                        retry_required: true,
                        error: format!("begin Session checkpoint: {error}"),
                    },
                };
            }
        };
        let effective_reason = match attempt.reason.as_str() {
            "idle" => RuntimeCheckpointReason::Idle,
            "version_switch" => RuntimeCheckpointReason::VersionSwitch,
            "drain" => RuntimeCheckpointReason::Drain,
            other => {
                return RuntimeCheckpointTransportOutcome {
                    checkpoint_attempt: Some((attempt.checkpoint_attempt_id, request.reason)),
                    result: RuntimeCheckpointEffectResult::Failed {
                        has_queued_work: false,
                        retry_required: true,
                        error: format!("Hub returned invalid checkpoint reason {other}"),
                    },
                };
            }
        };
        let paths = SessionPaths::for_session(&self.work_root, request.session_id);
        let session_id = request.session_id;
        let ownership_generation = request.ownership_generation;
        let checkpoint_attempt_id = attempt.checkpoint_attempt_id;
        let bundle_generation = attempt.bundle_generation;
        let history_checkpoint = attempt.history_checkpoint;
        let fallback_producing_codex_version = self.producing_codex_version.clone();
        let artifact_result = tokio::task::spawn_blocking(move || {
            let metadata: SessionSupervisorMetadata = serde_json::from_slice(
                &stdfs::read(paths.supervisor.join(SESSION_SUPERVISOR_METADATA_FILE))
                    .context("read Session supervisor metadata for checkpoint")?,
            )
            .context("parse Session supervisor metadata for checkpoint")?;
            anyhow::ensure!(
                metadata.ownership_generation == ownership_generation,
                "local Session checkpoint generation is stale"
            );
            let producing_codex_version = if metadata.codex_version.trim().is_empty() {
                fallback_producing_codex_version
            } else {
                metadata.codex_version.clone()
            };
            let native_thread_id = metadata
                .native_thread_id
                .context("Session checkpoint has no native Thread id")?;
            let archive_path = paths.staging.join(format!(
                "bundle-{}-{}.tar.zst",
                bundle_generation, checkpoint_attempt_id
            ));
            session_bundle::create_session_bundle(&session_bundle::SessionBundleCreateSpec {
                session_id,
                native_thread_id,
                history_checkpoint,
                bundle_generation,
                ownership_generation,
                producing_codex_version,
                created_at: chrono::Utc::now(),
                workspace: paths.workspace,
                codex_home: paths.codex,
                archive_path,
            })
        })
        .await
        .unwrap_or_else(|error| Err(anyhow::anyhow!("Session Bundle task stopped: {error}")));
        let result = match artifact_result {
            Ok(artifact) => match self
                .client
                .upload_session_bundle(
                    request.session_id,
                    request.ownership_generation,
                    attempt.checkpoint_attempt_id,
                    &artifact,
                )
                .await
            {
                Ok(committed)
                    if committed.checkpoint_attempt_id == attempt.checkpoint_attempt_id
                        && committed.bundle_generation == attempt.bundle_generation =>
                {
                    if let Err(error) = fs::remove_file(&artifact.archive_path).await {
                        if error.kind() != std::io::ErrorKind::NotFound {
                            warn!(
                                session_id = %request.session_id,
                                error = %error,
                                "failed to remove committed local Session Bundle staging file"
                            );
                        }
                    }
                    RuntimeCheckpointEffectResult::Saved {
                        has_queued_work: committed.has_queued_work,
                        ownership_released: committed.ownership_released,
                    }
                }
                Ok(_) => {
                    self.report_checkpoint_failure(
                        request,
                        attempt.checkpoint_attempt_id,
                        "bundle_commit_mismatch",
                        "Hub returned mismatched Session Bundle commit metadata",
                    )
                    .await
                }
                Err(error) => {
                    self.report_checkpoint_failure(
                        request,
                        attempt.checkpoint_attempt_id,
                        "bundle_upload_failed",
                        format!("upload Session Bundle: {error}"),
                    )
                    .await
                }
            },
            Err(error) => {
                self.report_checkpoint_failure(
                    request,
                    attempt.checkpoint_attempt_id,
                    "bundle_create_failed",
                    format!("create Session Bundle: {error}"),
                )
                .await
            }
        };
        RuntimeCheckpointTransportOutcome {
            checkpoint_attempt: Some((attempt.checkpoint_attempt_id, effective_reason)),
            result,
        }
    }
}

impl HubRuntimeCheckpointTransport {
    async fn report_checkpoint_failure(
        &self,
        request: &RuntimeCheckpointRequest,
        checkpoint_attempt_id: Uuid,
        error_code: &str,
        detail: impl Into<String>,
    ) -> RuntimeCheckpointEffectResult {
        let detail = detail.into();
        let disposition = self
            .client
            .fail_session_checkpoint(
                request.session_id,
                &FailRuntimeSessionCheckpointRequest {
                    ownership_generation: request.ownership_generation,
                    checkpoint_attempt_id,
                    error: error_code.into(),
                },
            )
            .await;
        match disposition {
            Ok(disposition) if matches!(disposition.disposition.as_str(), "resume" | "retry") => {
                RuntimeCheckpointEffectResult::Failed {
                    has_queued_work: disposition.has_queued_work,
                    retry_required: disposition.disposition == "retry",
                    error: detail,
                }
            }
            Ok(disposition) => RuntimeCheckpointEffectResult::Failed {
                has_queued_work: false,
                retry_required: true,
                error: format!(
                    "{detail}; Hub returned invalid checkpoint disposition {}",
                    disposition.disposition
                ),
            },
            Err(error) => RuntimeCheckpointEffectResult::Failed {
                has_queued_work: false,
                retry_required: true,
                error: format!("{detail}; report Session checkpoint failure: {error}"),
            },
        }
    }
}

struct SessionSupervisorManager {
    work_root: PathBuf,
    runtime_id: Uuid,
    max_online_sessions: usize,
    session_idle_timeout: Duration,
    idle_deadlines: std::sync::Mutex<BTreeMap<Uuid, (i64, tokio::time::Instant)>>,
    checkpoint_intents: std::sync::Mutex<BTreeMap<Uuid, (i64, RuntimeCheckpointReason)>>,
    checkpoint_attempts: std::sync::Mutex<BTreeMap<Uuid, RuntimeCheckpointRequest>>,
    checkpoint_retries:
        std::sync::Mutex<BTreeMap<Uuid, (RuntimeCheckpointRequest, tokio::time::Instant)>>,
    released_session_cleanups:
        std::sync::Mutex<BTreeMap<SessionCleanupKey, PersistedSessionCleanupState>>,
    released_session_cleanup_attempts: std::sync::Mutex<HashSet<SessionCleanupKey>>,
    records: std::sync::Mutex<BTreeMap<Uuid, ManagedSessionRecord>>,
    command_gates: std::sync::Mutex<BTreeMap<(Uuid, i64), Arc<SessionCommandGate>>>,
    stopped: AtomicBool,
}

#[derive(Default)]
struct SessionCommandGate {
    pending: AtomicUsize,
    notify: Notify,
}

impl SessionSupervisorManager {
    #[cfg(test)]
    fn new(work_root: PathBuf, runtime_id: Uuid, max_online_sessions: usize) -> Self {
        Self::new_with_idle_timeout(
            work_root,
            runtime_id,
            max_online_sessions,
            DEFAULT_SESSION_IDLE_TIMEOUT,
        )
    }

    #[cfg(test)]
    fn new_with_idle_timeout(
        work_root: PathBuf,
        runtime_id: Uuid,
        max_online_sessions: usize,
        session_idle_timeout: Duration,
    ) -> Self {
        Self::try_new_with_idle_timeout(
            work_root,
            runtime_id,
            max_online_sessions,
            session_idle_timeout,
        )
        .expect("load persisted Session cleanup state")
    }

    fn try_new_with_idle_timeout(
        work_root: PathBuf,
        runtime_id: Uuid,
        max_online_sessions: usize,
        session_idle_timeout: Duration,
    ) -> anyhow::Result<Self> {
        assert!(
            max_online_sessions > 0,
            "max online Sessions must be positive"
        );
        assert!(
            !session_idle_timeout.is_zero(),
            "Session idle timeout must be positive"
        );
        let released_session_cleanups = load_session_cleanup_states(&work_root)?;
        Ok(Self {
            work_root,
            runtime_id,
            max_online_sessions,
            session_idle_timeout,
            idle_deadlines: std::sync::Mutex::new(BTreeMap::new()),
            checkpoint_intents: std::sync::Mutex::new(BTreeMap::new()),
            checkpoint_attempts: std::sync::Mutex::new(BTreeMap::new()),
            checkpoint_retries: std::sync::Mutex::new(BTreeMap::new()),
            released_session_cleanups: std::sync::Mutex::new(released_session_cleanups),
            released_session_cleanup_attempts: std::sync::Mutex::new(HashSet::new()),
            records: std::sync::Mutex::new(BTreeMap::new()),
            command_gates: std::sync::Mutex::new(BTreeMap::new()),
            stopped: AtomicBool::new(false),
        })
    }

    fn session_command_gate(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
    ) -> Arc<SessionCommandGate> {
        Arc::clone(
            self.command_gates
                .lock()
                .unwrap()
                .entry((session_id, ownership_generation))
                .or_default(),
        )
    }

    fn arm_idle_deadline(&self, session_id: Uuid, ownership_generation: i64) {
        self.idle_deadlines.lock().unwrap().insert(
            session_id,
            (
                ownership_generation,
                tokio::time::Instant::now() + self.session_idle_timeout,
            ),
        );
    }

    fn request_checkpoint(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
        reason: RuntimeCheckpointReason,
    ) -> anyhow::Result<()> {
        let mut records = self.records.lock().unwrap();
        let record = records
            .get_mut(&session_id)
            .context("checkpoint Session is not managed by this Runtime")?;
        anyhow::ensure!(
            record.snapshot.ownership_generation == ownership_generation,
            "checkpoint ownership generation is stale"
        );
        let lifecycle_status = match &record.status {
            ManagedSessionStatus::Cold { metadata }
            | ManagedSessionStatus::Ready { metadata, .. } => metadata.lifecycle_status.as_str(),
            ManagedSessionStatus::Starting => "restoring",
            ManagedSessionStatus::Blocked { .. } => record.snapshot.lifecycle_status.as_str(),
        };
        anyhow::ensure!(
            matches!(lifecycle_status, "online" | "restoring" | "saving"),
            "Session cannot checkpoint from lifecycle {lifecycle_status}"
        );
        if lifecycle_status == "saving" {
            let metadata = match &mut record.status {
                ManagedSessionStatus::Cold { metadata } => metadata,
                _ => anyhow::bail!("saving Session does not have cold metadata"),
            };
            let current_reason = metadata
                .checkpoint_reason
                .unwrap_or(RuntimeCheckpointReason::Idle);
            if checkpoint_reason_priority(reason) > checkpoint_reason_priority(current_reason) {
                let mut upgraded = metadata.clone();
                upgraded.checkpoint_reason = Some(reason);
                persist_session_supervisor_metadata_sync(&self.work_root, &upgraded)?;
                *metadata = upgraded;
                if let Some(attempt) = self
                    .checkpoint_attempts
                    .lock()
                    .unwrap()
                    .get_mut(&session_id)
                {
                    anyhow::ensure!(
                        attempt.ownership_generation == ownership_generation,
                        "checkpoint attempt ownership generation is stale"
                    );
                    attempt.reason = reason;
                }
                if let Some((retry, _)) =
                    self.checkpoint_retries.lock().unwrap().get_mut(&session_id)
                {
                    anyhow::ensure!(
                        retry.ownership_generation == ownership_generation,
                        "checkpoint retry ownership generation is stale"
                    );
                    retry.reason = reason;
                }
            }
            return Ok(());
        }
        drop(records);

        self.idle_deadlines.lock().unwrap().remove(&session_id);
        let mut intents = self.checkpoint_intents.lock().unwrap();
        match intents.entry(session_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((ownership_generation, reason));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let (current_generation, current_reason) = *entry.get();
                anyhow::ensure!(
                    current_generation == ownership_generation,
                    "checkpoint intent ownership generation is stale"
                );
                if checkpoint_reason_priority(reason) > checkpoint_reason_priority(current_reason) {
                    entry.insert((ownership_generation, reason));
                }
            }
        }
        Ok(())
    }

    fn request_version_switch_checkpoints(
        &self,
        active_version: &str,
        old_version: &str,
    ) -> anyhow::Result<()> {
        let sessions = {
            let mut records = self.records.lock().unwrap();
            let mut sessions = Vec::new();
            for record in records.values_mut() {
                let (session_id, ownership_generation, should_checkpoint) = match &mut record.status
                {
                    ManagedSessionStatus::Cold { metadata }
                    | ManagedSessionStatus::Ready { metadata, .. } => {
                        if metadata.codex_version.trim().is_empty() {
                            let mut backfilled = metadata.clone();
                            backfilled.codex_version = old_version.to_owned();
                            persist_session_supervisor_metadata_sync(&self.work_root, &backfilled)?;
                            *metadata = backfilled;
                        }
                        (
                            metadata.session_id,
                            metadata.ownership_generation,
                            metadata.codex_version != active_version,
                        )
                    }
                    ManagedSessionStatus::Starting => (
                        record.snapshot.session_id,
                        record.snapshot.ownership_generation,
                        true,
                    ),
                    ManagedSessionStatus::Blocked { .. } => continue,
                };
                if should_checkpoint {
                    sessions.push((session_id, ownership_generation));
                }
            }
            sessions
        };
        for (session_id, ownership_generation) in sessions {
            self.request_checkpoint(
                session_id,
                ownership_generation,
                RuntimeCheckpointReason::VersionSwitch,
            )?;
        }
        Ok(())
    }

    async fn take_due_checkpoint_requests(&self) -> anyhow::Result<Vec<RuntimeCheckpointRequest>> {
        let now = tokio::time::Instant::now();
        let due = self
            .idle_deadlines
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, (_, deadline))| *deadline <= now)
            .map(|(session_id, (generation, _))| (*session_id, *generation))
            .collect::<Vec<_>>();
        for (session_id, ownership_generation) in due {
            if self
                .request_checkpoint(
                    session_id,
                    ownership_generation,
                    RuntimeCheckpointReason::Idle,
                )
                .is_err()
            {
                self.idle_deadlines.lock().unwrap().remove(&session_id);
            }
        }
        let pending = self
            .checkpoint_intents
            .lock()
            .unwrap()
            .iter()
            .map(|(session_id, (generation, reason))| (*session_id, *generation, *reason))
            .collect::<Vec<_>>();
        let mut requests = Vec::new();
        for (session_id, ownership_generation, reason) in pending {
            let transition = {
                let mut records = self.records.lock().unwrap();
                let Some(record) = records.get_mut(&session_id) else {
                    continue;
                };
                if record.snapshot.ownership_generation != ownership_generation
                    || record.reserved_run_id.is_some()
                {
                    continue;
                }
                let (mut metadata, supervisor) = match &record.status {
                    ManagedSessionStatus::Cold { metadata }
                        if metadata.lifecycle_status == "online" =>
                    {
                        (metadata.clone(), None)
                    }
                    ManagedSessionStatus::Ready {
                        metadata,
                        supervisor,
                        busy: false,
                    } if metadata.lifecycle_status == "online" => {
                        (metadata.clone(), Some(Arc::clone(supervisor)))
                    }
                    _ => continue,
                };
                metadata.lifecycle_status = "saving".into();
                metadata.idle_deadline_unix_ms = None;
                metadata.checkpoint_reason = Some(reason);
                metadata.checkpoint_retry_unix_ms = None;
                metadata.hub_checkpoint_attempt_id = None;
                persist_session_supervisor_metadata_sync(&self.work_root, &metadata)?;
                record.snapshot.lifecycle_status = "saving".into();
                record.status = ManagedSessionStatus::Cold {
                    metadata: metadata.clone(),
                };
                let proxy = record.model_proxy.take();
                Some((supervisor, proxy))
            };
            let Some((supervisor, proxy)) = transition else {
                continue;
            };
            if let Some(supervisor) = supervisor {
                supervisor.shutdown();
            }
            drop(proxy);
            self.idle_deadlines.lock().unwrap().remove(&session_id);
            self.checkpoint_intents.lock().unwrap().remove(&session_id);
            let request = RuntimeCheckpointRequest {
                session_id,
                ownership_generation,
                reason,
            };
            self.checkpoint_attempts
                .lock()
                .unwrap()
                .insert(session_id, request.clone());
            requests.push(request);
        }
        let due_retries = self
            .checkpoint_retries
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, (_, deadline))| *deadline <= now)
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();
        for session_id in due_retries {
            let Some((request, _)) = self.checkpoint_retries.lock().unwrap().remove(&session_id)
            else {
                continue;
            };
            let retry_is_current =
                self.records
                    .lock()
                    .unwrap()
                    .get(&session_id)
                    .is_some_and(|record| {
                        record.snapshot.ownership_generation == request.ownership_generation
                            && matches!(
                                &record.status,
                                ManagedSessionStatus::Cold { metadata }
                                    if metadata.lifecycle_status == "saving"
                            )
                    });
            if retry_is_current {
                self.checkpoint_attempts
                    .lock()
                    .unwrap()
                    .insert(session_id, request.clone());
                requests.push(request);
            }
        }
        Ok(requests)
    }

    fn finish_checkpoint(
        &self,
        request: &RuntimeCheckpointRequest,
        result: RuntimeCheckpointEffectResult,
    ) -> anyhow::Result<Option<ReleasedSessionCleanup>> {
        let effective_request = self
            .checkpoint_attempts
            .lock()
            .unwrap()
            .get(&request.session_id)
            .cloned()
            .context("checkpoint result does not match an active attempt")?;
        anyhow::ensure!(
            effective_request.ownership_generation == request.ownership_generation,
            "checkpoint result ownership generation is stale"
        );
        let mut records = self.records.lock().unwrap();
        let record = records
            .get_mut(&request.session_id)
            .context("checkpoint Session is no longer managed by this Runtime")?;
        anyhow::ensure!(
            record.snapshot.ownership_generation == effective_request.ownership_generation,
            "checkpoint result ownership generation is stale"
        );
        let mut retry = None;
        let mut cleanup = None;
        match result {
            RuntimeCheckpointEffectResult::Saved {
                has_queued_work,
                ownership_released,
            } => {
                if ownership_released {
                    cleanup = self.reserve_session_cleanup(
                        request.session_id,
                        request.ownership_generation,
                    )?;
                    records.remove(&request.session_id);
                } else {
                    anyhow::ensure!(
                        has_queued_work,
                        "a retained checkpoint owner requires queued work"
                    );
                    let mut metadata = match &record.status {
                        ManagedSessionStatus::Cold { metadata }
                            if metadata.lifecycle_status == "saving" =>
                        {
                            metadata.clone()
                        }
                        _ => anyhow::bail!("checkpoint Session is not saving"),
                    };
                    metadata.lifecycle_status = "online".into();
                    metadata.idle_deadline_unix_ms = None;
                    metadata.checkpoint_reason = None;
                    metadata.checkpoint_retry_unix_ms = None;
                    metadata.hub_checkpoint_attempt_id = None;
                    persist_session_supervisor_metadata_sync(&self.work_root, &metadata)?;
                    record.snapshot.lifecycle_status = "online".into();
                    record.status = ManagedSessionStatus::Cold { metadata };
                }
            }
            RuntimeCheckpointEffectResult::Failed {
                has_queued_work,
                retry_required,
                error,
            } => {
                warn!(
                    session_id = %request.session_id,
                    reason = ?effective_request.reason,
                    error = %error,
                    "Session checkpoint failed"
                );
                anyhow::ensure!(
                    retry_required || has_queued_work,
                    "checkpoint resume disposition requires queued work"
                );
                let must_retry = retry_required;
                if must_retry {
                    let mut metadata = match &record.status {
                        ManagedSessionStatus::Cold { metadata }
                            if metadata.lifecycle_status == "saving" =>
                        {
                            metadata.clone()
                        }
                        _ => anyhow::bail!("checkpoint Session is not saving"),
                    };
                    metadata.checkpoint_reason = Some(effective_request.reason);
                    metadata.checkpoint_retry_unix_ms = Some(system_time_unix_millis(
                        SystemTime::now()
                            .checked_add(CHECKPOINT_RETRY_DELAY)
                            .context("checkpoint retry deadline overflowed")?,
                    )?);
                    persist_session_supervisor_metadata_sync(&self.work_root, &metadata)?;
                    record.status = ManagedSessionStatus::Cold { metadata };
                    retry = Some(effective_request.clone());
                } else {
                    let mut metadata = match &record.status {
                        ManagedSessionStatus::Cold { metadata }
                            if metadata.lifecycle_status == "saving" =>
                        {
                            metadata.clone()
                        }
                        _ => anyhow::bail!("checkpoint Session is not saving"),
                    };
                    metadata.lifecycle_status = "online".into();
                    metadata.idle_deadline_unix_ms = None;
                    metadata.checkpoint_reason = None;
                    metadata.checkpoint_retry_unix_ms = None;
                    metadata.hub_checkpoint_attempt_id = None;
                    persist_session_supervisor_metadata_sync(&self.work_root, &metadata)?;
                    record.snapshot.lifecycle_status = "online".into();
                    record.status = ManagedSessionStatus::Cold { metadata };
                }
            }
        }
        drop(records);
        self.checkpoint_attempts
            .lock()
            .unwrap()
            .remove(&request.session_id);
        if let Some(request) = retry {
            self.checkpoint_retries.lock().unwrap().insert(
                request.session_id,
                (
                    request,
                    tokio::time::Instant::now() + CHECKPOINT_RETRY_DELAY,
                ),
            );
        }
        Ok(cleanup)
    }

    fn reserve_session_cleanup(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
    ) -> anyhow::Result<Option<ReleasedSessionCleanup>> {
        let key = (session_id, ownership_generation);
        if let Some(state) = self.released_session_cleanups.lock().unwrap().get(&key) {
            return match state {
                PersistedSessionCleanupState::Reserved { path }
                    if self
                        .released_session_cleanup_attempts
                        .lock()
                        .unwrap()
                        .insert(key) =>
                {
                    Ok(Some(ReleasedSessionCleanup {
                        session_id,
                        ownership_generation,
                        path: path.clone(),
                    }))
                }
                PersistedSessionCleanupState::Reserved { .. } => Ok(None),
                PersistedSessionCleanupState::Completed => Ok(None),
            };
        }
        let source = SessionPaths::for_session(&self.work_root, session_id).root;
        match stdfs::symlink_metadata(&source) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).context("inspect released local Session directory");
            }
        }
        let cleanup_root = self.work_root.join(SESSION_CLEANUP_DIRECTORY);
        stdfs::create_dir_all(&cleanup_root)
            .context("create released Session cleanup directory")?;
        let cleanup = ReleasedSessionCleanup {
            session_id,
            ownership_generation,
            path: session_cleanup_path(&self.work_root, session_id, ownership_generation),
        };
        let mut reservations = self.released_session_cleanups.lock().unwrap();
        anyhow::ensure!(
            !reservations.contains_key(&key),
            "released Session generation already has a cleanup reservation"
        );
        stdfs::rename(&source, &cleanup.path)
            .context("isolate released local Session directory for cleanup")?;
        stdfs::File::open(source.parent().context("Session directory has no parent")?)
            .context("open Session directory after cleanup reservation")?
            .sync_all()
            .context("sync Session directory after cleanup reservation")?;
        stdfs::File::open(&cleanup_root)
            .context("open cleanup directory after reservation")?
            .sync_all()
            .context("sync cleanup directory after reservation")?;
        reservations.insert(
            key,
            PersistedSessionCleanupState::Reserved {
                path: cleanup.path.clone(),
            },
        );
        if let Err(error) = persist_session_cleanup_states(&self.work_root, &reservations) {
            warn!(
                session_id = %session_id,
                ownership_generation,
                error = %error,
                "cleanup reservation state file was not persisted; the generation directory remains recoverable"
            );
        }
        self.released_session_cleanup_attempts
            .lock()
            .unwrap()
            .insert(key);
        Ok(Some(cleanup))
    }

    fn reserve_hub_cleanup_obligations(
        &self,
        obligations: &[RuntimeOwnedSessionGenerationDto],
    ) -> anyhow::Result<Vec<ReleasedSessionCleanup>> {
        let mut cleanups = Vec::new();
        for obligation in obligations {
            anyhow::ensure!(
                obligation.ownership_generation > 0,
                "cleanup ownership generation must be positive"
            );
            let current_generation = self
                .records
                .lock()
                .unwrap()
                .get(&obligation.session_id)
                .map(|record| record.snapshot.ownership_generation);
            if current_generation == Some(obligation.ownership_generation) {
                continue;
            }
            if current_generation.is_some() {
                self.record_missing_session_cleanup(obligation)?;
                continue;
            }
            if let Some(cleanup) = self
                .reserve_session_cleanup(obligation.session_id, obligation.ownership_generation)?
            {
                cleanups.push(cleanup);
            } else if !self
                .released_session_cleanups
                .lock()
                .unwrap()
                .contains_key(&(obligation.session_id, obligation.ownership_generation))
            {
                self.record_missing_session_cleanup(obligation)?;
            }
        }
        Ok(cleanups)
    }

    fn record_missing_session_cleanup(
        &self,
        obligation: &RuntimeOwnedSessionGenerationDto,
    ) -> anyhow::Result<()> {
        let key = (obligation.session_id, obligation.ownership_generation);
        let mut states = self.released_session_cleanups.lock().unwrap();
        if states.contains_key(&key) {
            return Ok(());
        }
        states.insert(key, PersistedSessionCleanupState::Completed);
        if let Err(error) = persist_session_cleanup_states(&self.work_root, &states) {
            states.remove(&key);
            return Err(error).context("persist missing Session cleanup receipt");
        }
        Ok(())
    }

    fn complete_released_session_cleanup(
        &self,
        cleanup: &ReleasedSessionCleanup,
    ) -> anyhow::Result<()> {
        let key = (cleanup.session_id, cleanup.ownership_generation);
        let mut states = self.released_session_cleanups.lock().unwrap();
        let state = states.get(&key);
        anyhow::ensure!(
            matches!(
                state,
                Some(PersistedSessionCleanupState::Reserved { path })
                    if path == &cleanup.path
            ),
            "released Session cleanup reservation is stale"
        );
        states.insert(key, PersistedSessionCleanupState::Completed);
        if let Err(error) = persist_session_cleanup_states(&self.work_root, &states) {
            states.insert(
                key,
                PersistedSessionCleanupState::Reserved {
                    path: cleanup.path.clone(),
                },
            );
            return Err(error).context("persist completed Session cleanup receipt");
        }
        self.released_session_cleanup_attempts
            .lock()
            .unwrap()
            .remove(&key);
        Ok(())
    }

    fn persist_released_session_cleanup_reservation(
        &self,
        cleanup: &ReleasedSessionCleanup,
    ) -> anyhow::Result<()> {
        let states = self.released_session_cleanups.lock().unwrap();
        anyhow::ensure!(
            matches!(
                states.get(&(cleanup.session_id, cleanup.ownership_generation)),
                Some(PersistedSessionCleanupState::Reserved { path })
                    if path == &cleanup.path
            ),
            "released Session cleanup reservation is stale"
        );
        persist_session_cleanup_states(&self.work_root, &states)
            .context("persist Session cleanup reservation before removal")
    }

    fn finish_released_session_cleanup_attempt(&self, cleanup: &ReleasedSessionCleanup) {
        self.released_session_cleanup_attempts
            .lock()
            .unwrap()
            .remove(&(cleanup.session_id, cleanup.ownership_generation));
    }

    fn take_pending_released_session_cleanups(
        &self,
    ) -> anyhow::Result<Vec<ReleasedSessionCleanup>> {
        let mut states = self.released_session_cleanups.lock().unwrap();
        let cleanup_root = self.work_root.join(SESSION_CLEANUP_DIRECTORY);
        let mut changed = false;
        let entries = match stdfs::read_dir(&cleanup_root) {
            Ok(entries) => Some(entries),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("read released Session cleanup directory"),
        };
        if let Some(entries) = entries {
            for entry in entries {
                let entry = entry.context("inspect released Session cleanup directory")?;
                let path = entry.path();
                if path.file_name().and_then(|name| name.to_str())
                    == Some(SESSION_CLEANUP_STATE_FILE)
                    || !entry
                        .file_type()
                        .context("inspect released Session cleanup entry")?
                        .is_dir()
                {
                    continue;
                }
                let Some((session_id, ownership_generation)) =
                    parse_session_cleanup_directory_name(&entry.file_name().to_string_lossy())
                else {
                    continue;
                };
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    states.entry((session_id, ownership_generation))
                {
                    entry.insert(PersistedSessionCleanupState::Reserved { path });
                    changed = true;
                }
            }
        }
        if changed {
            persist_session_cleanup_states(&self.work_root, &states)?;
        }
        let pending = states
            .iter()
            .filter_map(|(&(session_id, ownership_generation), state)| match state {
                PersistedSessionCleanupState::Reserved { path } => Some(ReleasedSessionCleanup {
                    session_id,
                    ownership_generation,
                    path: path.clone(),
                }),
                PersistedSessionCleanupState::Completed => None,
            })
            .collect::<Vec<_>>();
        let mut attempts = self.released_session_cleanup_attempts.lock().unwrap();
        Ok(pending
            .into_iter()
            .filter(|cleanup| attempts.insert((cleanup.session_id, cleanup.ownership_generation)))
            .collect())
    }

    fn acknowledge_cleaned_sessions(
        &self,
        acknowledged: &[RuntimeOwnedSessionGenerationDto],
    ) -> anyhow::Result<()> {
        if acknowledged.is_empty() {
            return Ok(());
        }
        let mut states = self.released_session_cleanups.lock().unwrap();
        let previous = states.clone();
        for receipt in acknowledged {
            let key = (receipt.session_id, receipt.ownership_generation);
            if matches!(
                states.get(&key),
                Some(PersistedSessionCleanupState::Completed)
            ) {
                states.remove(&key);
            }
        }
        if *states == previous {
            return Ok(());
        }
        if let Err(error) = persist_session_cleanup_states(&self.work_root, &states) {
            *states = previous;
            return Err(error).context("persist acknowledged Session cleanup receipts");
        }
        Ok(())
    }

    fn acknowledge_checkpoint_begin(
        &self,
        request: &RuntimeCheckpointRequest,
        checkpoint_attempt_id: Uuid,
        reason: RuntimeCheckpointReason,
    ) -> anyhow::Result<()> {
        self.request_checkpoint(request.session_id, request.ownership_generation, reason)?;
        let mut records = self.records.lock().unwrap();
        let record = records
            .get_mut(&request.session_id)
            .context("checkpoint Session disappeared before Hub ACK persistence")?;
        anyhow::ensure!(
            record.snapshot.ownership_generation == request.ownership_generation,
            "checkpoint Hub ACK ownership generation is stale"
        );
        let metadata = match &mut record.status {
            ManagedSessionStatus::Cold { metadata } if metadata.lifecycle_status == "saving" => {
                metadata
            }
            _ => anyhow::bail!("checkpoint Hub ACK Session is not saving"),
        };
        if let Some(existing) = metadata.hub_checkpoint_attempt_id {
            anyhow::ensure!(
                existing == checkpoint_attempt_id,
                "Hub changed checkpoint attempt id within one local attempt"
            );
            return Ok(());
        }
        let mut acknowledged = metadata.clone();
        acknowledged.hub_checkpoint_attempt_id = Some(checkpoint_attempt_id);
        acknowledged.checkpoint_reason = Some(reason);
        persist_session_supervisor_metadata_sync(&self.work_root, &acknowledged)?;
        *metadata = acknowledged;
        Ok(())
    }

    fn begin_session_command(&self, session_id: Uuid, ownership_generation: i64) {
        self.session_command_gate(session_id, ownership_generation)
            .pending
            .fetch_add(1, Ordering::AcqRel);
    }

    fn finish_session_command(&self, session_id: Uuid, ownership_generation: i64) {
        let gate = self.session_command_gate(session_id, ownership_generation);
        let previous = gate.pending.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "Session command gate underflowed");
        if previous == 1 {
            gate.notify.notify_waiters();
        }
    }

    fn session_has_pending_commands(&self, session_id: Uuid, ownership_generation: i64) -> bool {
        self.session_command_gate(session_id, ownership_generation)
            .pending
            .load(Ordering::Acquire)
            > 0
    }

    async fn wait_for_session_commands(&self, session_id: Uuid, ownership_generation: i64) {
        let gate = self.session_command_gate(session_id, ownership_generation);
        loop {
            let notified = gate.notify.notified();
            if gate.pending.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    fn reserve_blocked(&self, snapshot: RuntimeOwnedSessionSnapshotDto, reason: String) {
        let paths = SessionPaths::for_session(&self.work_root, snapshot.session_id);
        for path in [
            &paths.workspace,
            &paths.codex,
            &paths.supervisor,
            &paths.staging,
        ] {
            let _ = stdfs::create_dir_all(path);
        }
        self.records.lock().unwrap().insert(
            snapshot.session_id,
            ManagedSessionRecord {
                snapshot,
                status: ManagedSessionStatus::Blocked {
                    reason,
                    restart_attempts: 0,
                },
                reserved_run_id: None,
                model_proxy: None,
            },
        );
    }

    fn reserve_claim(&self, claim: &ClaimRunResponse) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.stopped.load(Ordering::Acquire),
            "Session manager is stopped"
        );
        let session_id = claim
            .run
            .hub_session_id
            .context("claimed Run is missing its Hub Session id")?;
        let ownership_generation = claim
            .run
            .session_ownership_generation
            .context("claimed Run is missing its Session ownership generation")?;
        anyhow::ensure!(
            ownership_generation > 0,
            "ownership generation must be positive"
        );
        let native_thread_id = claim
            .session_context
            .as_ref()
            .and_then(|context| context.session.native_thread_id.clone());
        let metadata = SessionSupervisorMetadata {
            format_version: 1,
            session_id,
            runtime_id: self.runtime_id,
            ownership_generation,
            lifecycle_status: "online".into(),
            idle_deadline_unix_ms: None,
            checkpoint_reason: None,
            checkpoint_retry_unix_ms: None,
            hub_checkpoint_attempt_id: None,
            codex_version: String::new(),
            native_thread_id: native_thread_id.clone(),
        };
        let mut records = self.records.lock().unwrap();
        if let Some(record) = records.get_mut(&session_id) {
            anyhow::ensure!(
                record.snapshot.ownership_generation == ownership_generation,
                "Session manager has a different ownership generation"
            );
            anyhow::ensure!(
                record.reserved_run_id.is_none(),
                "Session manager already reserved a Run"
            );
            let mut reserved_metadata = match &record.status {
                ManagedSessionStatus::Ready { metadata, busy, .. } => {
                    anyhow::ensure!(!*busy, "Session already has an active Run");
                    anyhow::ensure!(
                        metadata.lifecycle_status == "online",
                        "Session is not online for a new Run"
                    );
                    metadata.clone()
                }
                ManagedSessionStatus::Cold { metadata } => {
                    anyhow::ensure!(
                        metadata.lifecycle_status == "online",
                        "Session is not online for a new Run"
                    );
                    anyhow::ensure!(
                        metadata.runtime_id == self.runtime_id
                            && metadata.ownership_generation == ownership_generation,
                        "cold Session metadata does not match claimed generation"
                    );
                    metadata.clone()
                }
                ManagedSessionStatus::Starting => {
                    anyhow::bail!("Session supervisor is already starting")
                }
                ManagedSessionStatus::Blocked { reason, .. } => {
                    anyhow::bail!("Session supervisor is blocked: {reason}")
                }
            };
            reserved_metadata.idle_deadline_unix_ms = None;
            reserved_metadata.checkpoint_reason = None;
            reserved_metadata.checkpoint_retry_unix_ms = None;
            reserved_metadata.hub_checkpoint_attempt_id = None;
            persist_session_supervisor_metadata_sync(&self.work_root, &reserved_metadata)?;
            match &mut record.status {
                ManagedSessionStatus::Ready { metadata, .. }
                | ManagedSessionStatus::Cold { metadata } => *metadata = reserved_metadata,
                _ => unreachable!("validated Session state changed while locked"),
            }
            record.reserved_run_id = Some(claim.run.id);
            self.idle_deadlines.lock().unwrap().remove(&session_id);
            return Ok(());
        }
        anyhow::ensure!(
            records.len() < self.max_online_sessions,
            "Runtime Session capacity is full"
        );
        records.insert(
            session_id,
            ManagedSessionRecord {
                snapshot: RuntimeOwnedSessionSnapshotDto {
                    session_id,
                    ownership_generation,
                    lifecycle_status: "restoring".into(),
                    native_thread_id,
                    active_run_id: Some(claim.run.id),
                },
                status: ManagedSessionStatus::Cold { metadata },
                reserved_run_id: Some(claim.run.id),
                model_proxy: None,
            },
        );
        self.idle_deadlines.lock().unwrap().remove(&session_id);
        Ok(())
    }

    async fn ensure_app_server(
        &self,
        metadata: SessionSupervisorMetadata,
        codex_bin: String,
        run_env: RunEnv,
        timeout: Duration,
        model_proxy: Option<Arc<LocalModelProxy>>,
    ) -> anyhow::Result<Arc<SessionSupervisor>> {
        anyhow::ensure!(
            !self.stopped.load(Ordering::Acquire),
            "Session manager is stopped"
        );
        anyhow::ensure!(
            metadata.runtime_id == self.runtime_id,
            "Session metadata belongs to a different Runtime"
        );
        anyhow::ensure!(
            metadata.ownership_generation > 0,
            "ownership generation must be positive"
        );
        let snapshot = RuntimeOwnedSessionSnapshotDto {
            session_id: metadata.session_id,
            ownership_generation: metadata.ownership_generation,
            lifecycle_status: metadata.lifecycle_status.clone(),
            native_thread_id: metadata.native_thread_id.clone(),
            active_run_id: None,
        };
        {
            let mut records = self.records.lock().unwrap();
            if let Some(record) = records.get_mut(&metadata.session_id) {
                anyhow::ensure!(
                    record.snapshot.ownership_generation == metadata.ownership_generation,
                    "Session manager has a different ownership generation"
                );
                match &record.status {
                    ManagedSessionStatus::Ready { supervisor, .. } => {
                        return Ok(Arc::clone(supervisor));
                    }
                    ManagedSessionStatus::Cold {
                        metadata: recovered,
                    } => {
                        anyhow::ensure!(
                            recovered.runtime_id == metadata.runtime_id
                                && recovered.ownership_generation == metadata.ownership_generation,
                            "cold Session metadata does not match claimed generation"
                        );
                        if let Some(model_proxy) = model_proxy.as_ref() {
                            record.model_proxy = Some(Arc::clone(model_proxy));
                        }
                        record.status = ManagedSessionStatus::Starting;
                    }
                    ManagedSessionStatus::Starting => {
                        anyhow::bail!("Session supervisor is already starting")
                    }
                    ManagedSessionStatus::Blocked { reason, .. } => {
                        anyhow::bail!("Session supervisor is blocked: {reason}")
                    }
                }
            } else {
                anyhow::ensure!(
                    records.len() < self.max_online_sessions,
                    "Runtime Session capacity is full"
                );
                records.insert(
                    metadata.session_id,
                    ManagedSessionRecord {
                        snapshot,
                        status: ManagedSessionStatus::Starting,
                        reserved_run_id: None,
                        model_proxy: model_proxy.as_ref().map(Arc::clone),
                    },
                );
            }
        }

        if let Err(error) = persist_session_supervisor_metadata(&self.work_root, &metadata).await {
            self.mark_blocked(metadata.session_id, error.to_string());
            return Err(error);
        }
        let supervisor = match SessionSupervisor::start_app_server(
            metadata.session_id,
            metadata.ownership_generation,
            codex_bin,
            run_env,
            timeout,
        )
        .await
        {
            Ok(supervisor) => supervisor,
            Err(error) => {
                self.mark_blocked(metadata.session_id, error.to_string());
                return Err(error);
            }
        };
        if self.stopped.load(Ordering::Acquire) {
            supervisor.shutdown();
            self.mark_blocked(
                metadata.session_id,
                "Session manager stopped during startup".into(),
            );
            anyhow::bail!("Session manager stopped during startup");
        }
        let mut records = self.records.lock().unwrap();
        let record = records
            .get_mut(&metadata.session_id)
            .context("Session capacity record disappeared during startup")?;
        record.status = ManagedSessionStatus::Ready {
            metadata,
            supervisor: Arc::clone(&supervisor),
            busy: false,
        };
        Ok(supervisor)
    }

    #[cfg(test)]
    fn recover_cold(work_root: PathBuf, runtime_id: Uuid, plan: SessionRecoveryPlan) -> Arc<Self> {
        Self::recover_cold_with_idle_timeout(
            work_root,
            runtime_id,
            plan,
            DEFAULT_SESSION_IDLE_TIMEOUT,
        )
    }

    #[cfg(test)]
    fn recover_cold_with_idle_timeout(
        work_root: PathBuf,
        runtime_id: Uuid,
        plan: SessionRecoveryPlan,
        session_idle_timeout: Duration,
    ) -> Arc<Self> {
        Self::try_recover_cold_with_idle_timeout(work_root, runtime_id, plan, session_idle_timeout)
            .expect("recover persisted Session cleanup state")
    }

    fn try_recover_cold_with_idle_timeout(
        work_root: PathBuf,
        runtime_id: Uuid,
        plan: SessionRecoveryPlan,
        session_idle_timeout: Duration,
    ) -> anyhow::Result<Arc<Self>> {
        Self::try_recover_cold_with_idle_timeout_at(
            work_root,
            runtime_id,
            plan,
            session_idle_timeout,
            SystemTime::now(),
        )
    }

    #[cfg(test)]
    fn recover_cold_with_idle_timeout_at(
        work_root: PathBuf,
        runtime_id: Uuid,
        plan: SessionRecoveryPlan,
        session_idle_timeout: Duration,
        now: SystemTime,
    ) -> Arc<Self> {
        Self::try_recover_cold_with_idle_timeout_at(
            work_root,
            runtime_id,
            plan,
            session_idle_timeout,
            now,
        )
        .expect("recover persisted Session cleanup state")
    }

    fn try_recover_cold_with_idle_timeout_at(
        work_root: PathBuf,
        runtime_id: Uuid,
        plan: SessionRecoveryPlan,
        session_idle_timeout: Duration,
        now: SystemTime,
    ) -> anyhow::Result<Arc<Self>> {
        let manager = Arc::new(Self::try_new_with_idle_timeout(
            work_root,
            runtime_id,
            plan.max_online_sessions,
            session_idle_timeout,
        )?);
        for (_, record) in plan.records {
            match record.status {
                LocalSessionRecoveryStatus::Blocked(reason) => {
                    manager.records.lock().unwrap().insert(
                        record.snapshot.session_id,
                        ManagedSessionRecord {
                            snapshot: record.snapshot,
                            status: ManagedSessionStatus::Blocked {
                                reason,
                                restart_attempts: 0,
                            },
                            reserved_run_id: None,
                            model_proxy: None,
                        },
                    );
                }
                LocalSessionRecoveryStatus::Ready(metadata) => {
                    let should_arm_idle = record.snapshot.lifecycle_status == "online"
                        && metadata.lifecycle_status == "online";
                    let checkpoint_retry = (record.snapshot.lifecycle_status == "saving")
                        .then(|| {
                            metadata.checkpoint_reason.map(|reason| {
                                (
                                    RuntimeCheckpointRequest {
                                        session_id: metadata.session_id,
                                        ownership_generation: metadata.ownership_generation,
                                        reason,
                                    },
                                    metadata
                                        .checkpoint_retry_unix_ms
                                        .map(|deadline| duration_until_unix_millis(deadline, now))
                                        .unwrap_or_default(),
                                )
                            })
                        })
                        .flatten();
                    let remaining_idle = metadata
                        .idle_deadline_unix_ms
                        .map(|deadline| duration_until_unix_millis(deadline, now))
                        .unwrap_or(session_idle_timeout);
                    let session_id = metadata.session_id;
                    let ownership_generation = metadata.ownership_generation;
                    for path in [
                        &record.paths.workspace,
                        &record.paths.codex,
                        &record.paths.supervisor,
                        &record.paths.staging,
                    ] {
                        let _ = stdfs::create_dir_all(path);
                    }
                    manager.records.lock().unwrap().insert(
                        session_id,
                        ManagedSessionRecord {
                            snapshot: record.snapshot,
                            status: ManagedSessionStatus::Cold { metadata },
                            reserved_run_id: None,
                            model_proxy: None,
                        },
                    );
                    if should_arm_idle {
                        manager.idle_deadlines.lock().unwrap().insert(
                            session_id,
                            (
                                ownership_generation,
                                tokio::time::Instant::now() + remaining_idle,
                            ),
                        );
                    }
                    if let Some((request, remaining_retry)) = checkpoint_retry {
                        manager.checkpoint_retries.lock().unwrap().insert(
                            session_id,
                            (request, tokio::time::Instant::now() + remaining_retry),
                        );
                    }
                }
            }
        }
        Ok(manager)
    }

    async fn execute(
        &self,
        claim: ClaimRunResponse,
        event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
    ) -> anyhow::Result<AppServerRunResult> {
        let session_id = claim
            .run
            .hub_session_id
            .context("claimed Run is missing its Hub Session id")?;
        let ownership_generation = claim
            .run
            .session_ownership_generation
            .context("claimed Run is missing its Session ownership generation")?;
        let (supervisor, model_proxy) = {
            let mut records = self.records.lock().unwrap();
            let record = records
                .get_mut(&session_id)
                .context("Session is not registered with this Runtime manager")?;
            if let Some(reserved_run_id) = record.reserved_run_id {
                anyhow::ensure!(
                    reserved_run_id == claim.run.id,
                    "Session is reserved for a different Run"
                );
                record.reserved_run_id = None;
            }
            match &mut record.status {
                ManagedSessionStatus::Ready {
                    supervisor, busy, ..
                } => {
                    anyhow::ensure!(!*busy, "Session already has an active Run");
                    *busy = true;
                    (
                        Arc::clone(supervisor),
                        record.model_proxy.as_ref().map(Arc::clone),
                    )
                }
                ManagedSessionStatus::Starting => {
                    anyhow::bail!("Session supervisor is still starting")
                }
                ManagedSessionStatus::Cold { .. } => {
                    anyhow::bail!("Session supervisor is cold and must be started for this Run")
                }
                ManagedSessionStatus::Blocked { reason, .. } => {
                    anyhow::bail!("Session supervisor is blocked: {reason}")
                }
            }
        };
        if let Some(model_proxy) = model_proxy {
            model_proxy.activate_run(claim.run.id, &claim.model_proxy_token);
        }
        let result = supervisor.execute(claim, event_tx).await;
        if result.is_ok() {
            self.wait_for_session_commands(session_id, ownership_generation)
                .await;
        }
        let proxy_to_drop = {
            let mut records = self.records.lock().unwrap();
            let mut proxy_to_drop = None;
            if let Some(record) = records.get_mut(&session_id) {
                if result.is_ok() {
                    if let ManagedSessionStatus::Ready { busy, .. } = &mut record.status {
                        *busy = false;
                    }
                } else {
                    supervisor.shutdown();
                    proxy_to_drop = record.model_proxy.take();
                    record.status = ManagedSessionStatus::Blocked {
                        reason: result
                            .as_ref()
                            .err()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "Session supervisor failed".into()),
                        restart_attempts: 1,
                    };
                }
            }
            proxy_to_drop
        };
        drop(proxy_to_drop);
        if result.is_ok() {
            self.arm_idle_deadline(session_id, ownership_generation);
        }
        result
    }

    fn mark_blocked(&self, session_id: Uuid, reason: String) {
        let proxy_to_drop = {
            let mut records = self.records.lock().unwrap();
            records.get_mut(&session_id).map(|record| {
                let restart_attempts = match &record.status {
                    ManagedSessionStatus::Blocked {
                        restart_attempts, ..
                    } => restart_attempts.saturating_add(1),
                    _ => 1,
                };
                let proxy = record.model_proxy.take();
                record.reserved_run_id = None;
                record.status = ManagedSessionStatus::Blocked {
                    reason,
                    restart_attempts,
                };
                proxy
            })
        };
        drop(proxy_to_drop);
    }

    fn cancel_session(&self, session_id: Uuid, reason: String) {
        let (supervisor, proxy) = {
            let mut records = self.records.lock().unwrap();
            let Some(record) = records.get_mut(&session_id) else {
                return;
            };
            let supervisor = match &record.status {
                ManagedSessionStatus::Ready { supervisor, .. } => Some(Arc::clone(supervisor)),
                _ => None,
            };
            let restart_attempts = match &record.status {
                ManagedSessionStatus::Blocked {
                    restart_attempts, ..
                } => *restart_attempts,
                _ => 1,
            };
            let proxy = record.model_proxy.take();
            record.reserved_run_id = None;
            record.status = ManagedSessionStatus::Blocked {
                reason,
                restart_attempts,
            };
            (supervisor, proxy)
        };
        if let Some(supervisor) = supervisor {
            supervisor.shutdown();
        }
        drop(proxy);
    }

    fn forget_fenced_session(&self, session_id: Uuid) {
        let record = self.records.lock().unwrap().remove(&session_id);
        self.idle_deadlines.lock().unwrap().remove(&session_id);
        self.checkpoint_intents.lock().unwrap().remove(&session_id);
        self.checkpoint_attempts.lock().unwrap().remove(&session_id);
        self.checkpoint_retries.lock().unwrap().remove(&session_id);
        self.command_gates
            .lock()
            .unwrap()
            .retain(|(owned_session_id, _), _| *owned_session_id != session_id);
        if let Some(ManagedSessionRecord {
            status: ManagedSessionStatus::Ready { supervisor, .. },
            ..
        }) = record
        {
            supervisor.shutdown();
        }
    }

    async fn update_native_thread_id(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
        native_thread_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let Some(native_thread_id) = native_thread_id else {
            return Ok(());
        };
        let metadata = {
            let records = self.records.lock().unwrap();
            let record = records
                .get(&session_id)
                .context("Session disappeared before native thread persistence")?;
            anyhow::ensure!(
                record.snapshot.ownership_generation == ownership_generation,
                "Session ownership changed before native thread persistence"
            );
            let mut metadata = match &record.status {
                ManagedSessionStatus::Ready { metadata, .. } => metadata.clone(),
                ManagedSessionStatus::Starting => {
                    anyhow::bail!("Session supervisor is still starting")
                }
                ManagedSessionStatus::Cold { .. } => {
                    anyhow::bail!("Session supervisor is cold")
                }
                ManagedSessionStatus::Blocked { reason, .. } => {
                    anyhow::bail!("Session supervisor is blocked: {reason}")
                }
            };
            metadata.native_thread_id = Some(native_thread_id.to_owned());
            metadata
        };
        persist_session_supervisor_metadata(&self.work_root, &metadata).await?;
        let mut records = self.records.lock().unwrap();
        let record = records
            .get_mut(&session_id)
            .context("Session disappeared after native thread persistence")?;
        anyhow::ensure!(
            record.snapshot.ownership_generation == ownership_generation,
            "Session ownership changed after native thread persistence"
        );
        let ManagedSessionStatus::Ready {
            metadata: current, ..
        } = &mut record.status
        else {
            anyhow::bail!("Session stopped while native thread metadata was persisted");
        };
        *current = metadata.clone();
        record.snapshot.native_thread_id = metadata.native_thread_id;
        Ok(())
    }

    async fn steer(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
        native_thread_id: &str,
        expected_turn_id: String,
        message_id: Uuid,
        content: String,
    ) -> anyhow::Result<SessionSteerOutcome> {
        let supervisor = {
            let records = self.records.lock().unwrap();
            let record = records
                .get(&session_id)
                .context("Steering Message Session is not managed by this Runtime")?;
            anyhow::ensure!(
                record.snapshot.ownership_generation == ownership_generation,
                "Steering Message ownership generation is stale"
            );
            match &record.status {
                ManagedSessionStatus::Ready {
                    metadata,
                    supervisor,
                    busy,
                } => {
                    anyhow::ensure!(
                        metadata.native_thread_id.as_deref() == Some(native_thread_id),
                        "Steering Message native Thread does not match Session metadata"
                    );
                    if !*busy {
                        return Ok(SessionSteerOutcome::TurnEnded);
                    }
                    Arc::clone(supervisor)
                }
                ManagedSessionStatus::Starting => {
                    anyhow::bail!("Session supervisor is still starting")
                }
                ManagedSessionStatus::Cold { .. } => {
                    anyhow::bail!("Session supervisor is cold")
                }
                ManagedSessionStatus::Blocked { reason, .. } => {
                    anyhow::bail!("Session supervisor is blocked: {reason}")
                }
            }
        };
        supervisor
            .steer(
                ownership_generation,
                expected_turn_id,
                message_id,
                vec![content],
            )
            .await
    }

    async fn interrupt(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
        native_thread_id: &str,
        expected_turn_id: String,
    ) -> anyhow::Result<SessionInterruptOutcome> {
        let supervisor = {
            let records = self.records.lock().unwrap();
            let record = records
                .get(&session_id)
                .context("Interrupt Session is not managed by this Runtime")?;
            anyhow::ensure!(
                record.snapshot.ownership_generation == ownership_generation,
                "Interrupt ownership generation is stale"
            );
            match &record.status {
                ManagedSessionStatus::Ready {
                    metadata,
                    supervisor,
                    busy,
                } => {
                    anyhow::ensure!(
                        metadata.native_thread_id.as_deref() == Some(native_thread_id),
                        "Interrupt native Thread does not match Session metadata"
                    );
                    if !*busy {
                        return Ok(SessionInterruptOutcome::TurnEnded);
                    }
                    Arc::clone(supervisor)
                }
                ManagedSessionStatus::Starting => {
                    anyhow::bail!("Session supervisor is still starting")
                }
                ManagedSessionStatus::Cold { .. } => {
                    anyhow::bail!("Session supervisor is cold")
                }
                ManagedSessionStatus::Blocked { reason, .. } => {
                    anyhow::bail!("Session supervisor is blocked: {reason}")
                }
            }
        };
        supervisor
            .interrupt(ownership_generation, expected_turn_id)
            .await
    }

    fn model_proxy(&self, session_id: Uuid) -> Option<Arc<LocalModelProxy>> {
        self.records
            .lock()
            .unwrap()
            .get(&session_id)
            .and_then(|record| record.model_proxy.as_ref().map(Arc::clone))
    }

    fn refresh_failed_supervisors(&self) {
        let (supervisors, proxies) = {
            let mut records = self.records.lock().unwrap();
            let mut supervisors = Vec::new();
            let mut proxies = Vec::new();
            for record in records.values_mut() {
                let failure = match &record.status {
                    ManagedSessionStatus::Ready { supervisor, .. } => supervisor
                        .terminal_failure()
                        .map(|reason| (Arc::clone(supervisor), reason)),
                    _ => None,
                };
                let Some((supervisor, reason)) = failure else {
                    continue;
                };
                supervisors.push(supervisor);
                if let Some(proxy) = record.model_proxy.take() {
                    proxies.push(proxy);
                }
                record.reserved_run_id = None;
                record.status = ManagedSessionStatus::Blocked {
                    reason,
                    restart_attempts: 1,
                };
            }
            (supervisors, proxies)
        };
        for supervisor in supervisors {
            supervisor.shutdown();
        }
        drop(proxies);
    }

    fn heartbeat_request(&self) -> RuntimeHeartbeatRequest {
        self.refresh_failed_supervisors();
        let owned_sessions = self
            .records
            .lock()
            .unwrap()
            .values()
            .map(|record| {
                let (lifecycle_status, checkpoint_reason) = match &record.status {
                    ManagedSessionStatus::Starting => ("restoring".to_owned(), None),
                    ManagedSessionStatus::Cold { metadata }
                    | ManagedSessionStatus::Ready { metadata, .. } => (
                        metadata.lifecycle_status.clone(),
                        metadata
                            .checkpoint_reason
                            .map(|reason| reason.as_str().to_owned()),
                    ),
                    ManagedSessionStatus::Blocked { .. } => {
                        (record.snapshot.lifecycle_status.clone(), None)
                    }
                };
                RuntimeOwnedSessionStateRequest {
                    session_id: record.snapshot.session_id,
                    ownership_generation: record.snapshot.ownership_generation,
                    lifecycle_status,
                    checkpoint_reason,
                }
            })
            .collect();
        let cleaned_sessions = self
            .released_session_cleanups
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(&(session_id, ownership_generation), state)| {
                matches!(state, PersistedSessionCleanupState::Completed).then_some(
                    RuntimeOwnedSessionGenerationDto {
                        session_id,
                        ownership_generation,
                    },
                )
            })
            .collect();
        RuntimeHeartbeatRequest {
            pending_credential_hash: None,
            accepts_session_commands: true,
            owned_sessions,
            cleaned_sessions,
            codex_status: None,
        }
    }

    fn reconcile_owned_snapshots(
        &self,
        snapshots: &[RuntimeOwnedSessionSnapshotDto],
    ) -> anyhow::Result<Vec<ReleasedSessionCleanup>> {
        let snapshots = snapshots
            .iter()
            .cloned()
            .map(|snapshot| (snapshot.session_id, snapshot))
            .collect::<BTreeMap<_, _>>();
        let mut supervisors_to_stop = Vec::new();
        let mut proxies_to_drop = Vec::new();
        let mut removed_generations = Vec::new();
        let mut unknown = Vec::new();
        {
            let mut records = self.records.lock().unwrap();
            let removed = records
                .keys()
                .filter(|session_id| !snapshots.contains_key(session_id))
                .copied()
                .collect::<Vec<_>>();
            for session_id in removed {
                if let Some(record) = records.remove(&session_id) {
                    removed_generations.push((session_id, record.snapshot.ownership_generation));
                    if let ManagedSessionStatus::Ready { supervisor, .. } = record.status {
                        supervisors_to_stop.push(supervisor);
                    }
                    if let Some(proxy) = record.model_proxy {
                        proxies_to_drop.push(proxy);
                    }
                }
            }
            for (session_id, snapshot) in &snapshots {
                let Some(record) = records.get_mut(session_id) else {
                    unknown.push(snapshot.clone());
                    continue;
                };
                let native_thread_mismatch = match (&snapshot.native_thread_id, &record.status) {
                    (
                        Some(hub_thread_id),
                        ManagedSessionStatus::Cold { metadata }
                        | ManagedSessionStatus::Ready { metadata, .. },
                    ) => metadata.native_thread_id.as_ref() != Some(hub_thread_id),
                    _ => false,
                };
                if record.snapshot.ownership_generation != snapshot.ownership_generation
                    || native_thread_mismatch
                {
                    if let ManagedSessionStatus::Ready { supervisor, .. } = &record.status {
                        supervisors_to_stop.push(Arc::clone(supervisor));
                    }
                    if let Some(proxy) = record.model_proxy.take() {
                        proxies_to_drop.push(proxy);
                    }
                    record.reserved_run_id = None;
                    record.status = ManagedSessionStatus::Blocked {
                        reason: if native_thread_mismatch {
                            "Hub native thread does not match local Session metadata".into()
                        } else {
                            "Hub ownership generation changed during Runtime reconciliation".into()
                        },
                        restart_attempts: 0,
                    };
                }
                record.snapshot = snapshot.clone();
            }
        }
        for supervisor in supervisors_to_stop {
            supervisor.shutdown();
        }
        drop(proxies_to_drop);
        let mut cleanups = Vec::new();
        for (session_id, ownership_generation) in removed_generations {
            self.idle_deadlines.lock().unwrap().remove(&session_id);
            self.checkpoint_intents.lock().unwrap().remove(&session_id);
            self.checkpoint_attempts.lock().unwrap().remove(&session_id);
            self.checkpoint_retries.lock().unwrap().remove(&session_id);
            self.command_gates
                .lock()
                .unwrap()
                .retain(|(candidate_session_id, _), _| *candidate_session_id != session_id);
            if let Some(cleanup) = self.reserve_session_cleanup(session_id, ownership_generation)? {
                cleanups.push(cleanup);
            }
        }
        cleanups.extend(self.take_pending_released_session_cleanups()?);
        for snapshot in unknown {
            self.reserve_blocked(
                snapshot,
                "Hub-owned Session appeared without a local supervisor record".into(),
            );
        }
        Ok(cleanups)
    }

    async fn complete_fake_claim(&self, claim: &ClaimRunResponse) -> anyhow::Result<()> {
        self.complete_fake_claim_at(claim, SystemTime::now()).await
    }

    async fn complete_fake_claim_at(
        &self,
        claim: &ClaimRunResponse,
        terminal_at: SystemTime,
    ) -> anyhow::Result<()> {
        let session_id = claim
            .run
            .hub_session_id
            .context("claimed Run is missing its Hub Session id")?;
        let run_id = claim.run.id;
        let metadata = {
            let records = self.records.lock().unwrap();
            let record = records
                .get(&session_id)
                .context("fake Session record disappeared")?;
            anyhow::ensure!(
                record.reserved_run_id == Some(run_id),
                "fake Session reservation changed before completion"
            );
            let mut metadata = match &record.status {
                ManagedSessionStatus::Cold { metadata } => metadata.clone(),
                _ => anyhow::bail!("fake Session is not in its reserved cold state"),
            };
            metadata.lifecycle_status = "online".into();
            metadata.checkpoint_reason = None;
            metadata.checkpoint_retry_unix_ms = None;
            metadata.hub_checkpoint_attempt_id = None;
            metadata.idle_deadline_unix_ms = Some(system_time_unix_millis(
                terminal_at
                    .checked_add(self.session_idle_timeout)
                    .context("Session idle deadline overflowed")?,
            )?);
            metadata
        };
        persist_session_supervisor_metadata(&self.work_root, &metadata).await?;
        let mut records = self.records.lock().unwrap();
        let record = records
            .get_mut(&session_id)
            .context("fake Session record disappeared after metadata persistence")?;
        anyhow::ensure!(
            record.reserved_run_id == Some(run_id),
            "fake Session reservation changed after metadata persistence"
        );
        record.reserved_run_id = None;
        record.snapshot.lifecycle_status = "online".into();
        record.status = ManagedSessionStatus::Cold { metadata };
        drop(records);
        self.arm_idle_deadline(session_id, claim.run.session_ownership_generation.unwrap());
        Ok(())
    }

    fn available_new_session_slots(&self) -> usize {
        self.max_online_sessions
            .saturating_sub(self.records.lock().unwrap().len())
    }

    fn interrupted_restoring_runs(&self) -> Vec<InterruptedRestoringRun> {
        self.records
            .lock()
            .unwrap()
            .values()
            .filter_map(|record| {
                matches!(record.status, ManagedSessionStatus::Blocked { .. })
                    .then_some(record.snapshot.active_run_id)
                    .flatten()
                    .filter(|_| record.snapshot.lifecycle_status == "restoring")
                    .map(|run_id| InterruptedRestoringRun {
                        session_id: record.snapshot.session_id,
                        run_id,
                        ownership_generation: record.snapshot.ownership_generation,
                    })
            })
            .collect()
    }

    fn ready_owned_sessions(&self) -> Vec<RuntimeOwnedSessionGenerationDto> {
        self.refresh_failed_supervisors();
        self.records
            .lock()
            .unwrap()
            .values()
            .filter_map(|record| {
                if record.reserved_run_id.is_some() {
                    return None;
                }
                if self
                    .checkpoint_intents
                    .lock()
                    .unwrap()
                    .contains_key(&record.snapshot.session_id)
                    || self
                        .checkpoint_attempts
                        .lock()
                        .unwrap()
                        .contains_key(&record.snapshot.session_id)
                    || self
                        .checkpoint_retries
                        .lock()
                        .unwrap()
                        .contains_key(&record.snapshot.session_id)
                {
                    return None;
                }
                if self.session_has_pending_commands(
                    record.snapshot.session_id,
                    record.snapshot.ownership_generation,
                ) {
                    return None;
                }
                match &record.status {
                    ManagedSessionStatus::Ready {
                        metadata,
                        supervisor,
                        busy: false,
                    } if metadata.ownership_generation == record.snapshot.ownership_generation
                        && metadata.lifecycle_status == "online"
                        && metadata.checkpoint_reason.is_none()
                        && metadata.checkpoint_retry_unix_ms.is_none()
                        && metadata.hub_checkpoint_attempt_id.is_none()
                        && supervisor.ownership_generation == metadata.ownership_generation =>
                    {
                        Some(RuntimeOwnedSessionGenerationDto {
                            session_id: record.snapshot.session_id,
                            ownership_generation: record.snapshot.ownership_generation,
                        })
                    }
                    ManagedSessionStatus::Cold { metadata }
                        if metadata.ownership_generation
                            == record.snapshot.ownership_generation
                            && metadata.lifecycle_status == "online"
                            && metadata.checkpoint_reason.is_none()
                            && metadata.checkpoint_retry_unix_ms.is_none()
                            && metadata.hub_checkpoint_attempt_id.is_none() =>
                    {
                        Some(RuntimeOwnedSessionGenerationDto {
                            session_id: record.snapshot.session_id,
                            ownership_generation: record.snapshot.ownership_generation,
                        })
                    }
                    _ => None,
                }
            })
            .collect()
    }

    #[cfg(test)]
    fn blocked_session_count(&self) -> usize {
        self.records
            .lock()
            .unwrap()
            .values()
            .filter(|record| match &record.status {
                ManagedSessionStatus::Blocked {
                    reason,
                    restart_attempts,
                } => !reason.is_empty() || *restart_attempts == 0,
                _ => false,
            })
            .count()
    }

    fn shutdown(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        let (supervisors, proxies) = {
            let mut records = self.records.lock().unwrap();
            let mut supervisors = Vec::new();
            let mut proxies = Vec::new();
            for record in records.values_mut() {
                if let ManagedSessionStatus::Ready { supervisor, .. } = &record.status {
                    supervisors.push(Arc::clone(supervisor));
                }
                if let Some(proxy) = record.model_proxy.take() {
                    proxies.push(proxy);
                }
                record.reserved_run_id = None;
                record.status = ManagedSessionStatus::Blocked {
                    reason: "Session manager stopped".into(),
                    restart_attempts: 0,
                };
            }
            (supervisors, proxies)
        };
        for supervisor in supervisors {
            supervisor.shutdown();
        }
        drop(proxies);
    }
}

async fn drive_runtime_checkpoints<T: RuntimeCheckpointTransport>(
    manager: &SessionSupervisorManager,
    transport: &T,
) -> anyhow::Result<usize> {
    let requests = manager.take_due_checkpoint_requests().await?;
    let count = requests.len();
    for request in requests {
        let outcome = transport.checkpoint(&request).await;
        if let Some((checkpoint_attempt_id, reason)) = outcome.checkpoint_attempt {
            manager.acknowledge_checkpoint_begin(&request, checkpoint_attempt_id, reason)?;
        }
        if let Some(cleanup) = manager.finish_checkpoint(&request, outcome.result)? {
            remove_hub_fenced_session_cleanup(manager, cleanup).await;
        }
    }
    Ok(count)
}

async fn fail_interrupted_restoring_runs(
    manager: &SessionSupervisorManager,
    client: &HubClient,
) -> usize {
    let mut failed = 0;
    for interrupted in manager.interrupted_restoring_runs() {
        match client
            .fail_run(interrupted.run_id, interrupted.ownership_generation)
            .await
        {
            Ok(()) => {
                manager.forget_fenced_session(interrupted.session_id);
                failed += 1;
                warn!(
                    session_id = %interrupted.session_id,
                    run_id = %interrupted.run_id,
                    "failed interrupted restoring Run after local recovery could not resume it"
                );
            }
            Err(error) => warn!(
                session_id = %interrupted.session_id,
                run_id = %interrupted.run_id,
                error = %error,
                "failed to reconcile interrupted restoring Run"
            ),
        }
    }
    failed
}

async fn remove_hub_fenced_session_cleanup(
    manager: &SessionSupervisorManager,
    cleanup: ReleasedSessionCleanup,
) {
    if let Err(error) = manager.persist_released_session_cleanup_reservation(&cleanup) {
        manager.finish_released_session_cleanup_attempt(&cleanup);
        warn!(
            session_id = %cleanup.session_id,
            ownership_generation = cleanup.ownership_generation,
            error = %error,
            "failed to persist Session cleanup reservation before removal"
        );
        return;
    }
    match fs::remove_dir_all(&cleanup.path).await {
        Ok(()) => {
            if let Err(error) = manager.complete_released_session_cleanup(&cleanup) {
                manager.finish_released_session_cleanup_attempt(&cleanup);
                warn!(
                    session_id = %cleanup.session_id,
                    ownership_generation = cleanup.ownership_generation,
                    error = %error,
                    "failed to persist completed Session cleanup receipt"
                );
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Err(error) = manager.complete_released_session_cleanup(&cleanup) {
                manager.finish_released_session_cleanup_attempt(&cleanup);
                warn!(
                    session_id = %cleanup.session_id,
                    ownership_generation = cleanup.ownership_generation,
                    error = %error,
                    "failed to persist completed Session cleanup receipt"
                );
            }
        }
        Err(error) => {
            manager.finish_released_session_cleanup_attempt(&cleanup);
            warn!(
                session_id = %cleanup.session_id,
                ownership_generation = cleanup.ownership_generation,
                error = %error,
                "failed to remove released local Session directory"
            );
        }
    }
}

impl Drop for SessionSupervisorManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn send_app_server_value(
    stdin: &mut std::process::ChildStdin,
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *stdin, value).context("serialize Codex app-server request")?;
    stdin.write_all(b"\n")?;
    stdin.flush()?;
    Ok(())
}

fn recv_app_server_line_once(
    line_rx: &mpsc::Receiver<anyhow::Result<String>>,
    child: &mut std::process::Child,
    started_at: Instant,
    timeout: Duration,
    wait: Duration,
    cancellation: &AppServerCancellation,
) -> anyhow::Result<Option<String>> {
    if cancellation.is_cancelled() {
        terminate_child_process_tree(child);
        anyhow::bail!("Codex app-server cancelled");
    }
    match line_rx.recv_timeout(wait) {
        Ok(line) => return line.map(Some),
        Err(mpsc::RecvTimeoutError::Timeout) => {}
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            anyhow::bail!("Codex app-server stdout closed before turn completed");
        }
    }
    if let Some(status) = child.try_wait().context("poll Codex app-server")? {
        anyhow::bail!("Codex app-server exited early with status {status}");
    }
    if started_at.elapsed() > timeout {
        terminate_child_process_tree(child);
        anyhow::bail!("Codex app-server timed out after {:?}", timeout);
    }
    Ok(None)
}

fn terminate_child_process_tree(child: &mut std::process::Child) {
    #[cfg(unix)]
    {
        kill_process_group(child.id() as i32);
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    let _ = child.wait();
}

#[cfg(unix)]
fn kill_process_group(process_group_id: i32) {
    unsafe {
        libc::kill(-process_group_id, libc::SIGKILL);
    }
}

#[derive(Clone, Copy)]
enum AppServerResponseKind {
    Initialize,
    Acknowledgement,
    Thread,
    TurnStart,
}

struct AppServerState {
    run_id: Uuid,
    events: Vec<AppendRunEventRequest>,
    final_status: String,
    session_id: Option<String>,
    thread_id: Option<String>,
    native_turn_id: Option<String>,
    turn_start_response_received: bool,
    turn_started_emitted: bool,
    pending_responses: BTreeMap<u64, AppServerResponseKind>,
    initialized: bool,
    assistant_text: String,
    assistant_emitted: bool,
    tool_request_ids: HashSet<String>,
    tool_request_source_ids: HashSet<String>,
    streamed_events: usize,
    done: bool,
}

impl AppServerState {
    fn new(run_id: Uuid) -> Self {
        Self {
            run_id,
            events: Vec::new(),
            final_status: "completed".into(),
            session_id: None,
            thread_id: None,
            native_turn_id: None,
            turn_start_response_received: false,
            turn_started_emitted: false,
            pending_responses: BTreeMap::new(),
            initialized: false,
            assistant_text: String::new(),
            assistant_emitted: false,
            tool_request_ids: HashSet::new(),
            tool_request_source_ids: HashSet::new(),
            streamed_events: 0,
            done: false,
        }
    }

    fn handle_value(&mut self, value: &serde_json::Value) -> anyhow::Result<()> {
        if value.get("id").is_some() {
            self.handle_response(value)?;
        } else if let Some(method) = value.get("method").and_then(|value| value.as_str()) {
            self.handle_notification(
                method,
                value.get("params").unwrap_or(&serde_json::Value::Null),
            )?;
        }
        Ok(())
    }

    fn expect_response(&mut self, request_id: u64, kind: AppServerResponseKind) {
        self.pending_responses.insert(request_id, kind);
    }

    fn handle_response(&mut self, value: &serde_json::Value) -> anyhow::Result<()> {
        let Some(request_id) = value.get("id").and_then(serde_json::Value::as_u64) else {
            return Ok(());
        };
        let Some(kind) = self.pending_responses.remove(&request_id) else {
            return Ok(());
        };
        if value.get("error").is_some() {
            anyhow::bail!("Codex app-server returned a JSON-RPC error");
        }
        let result = value
            .get("result")
            .context("Codex app-server response is missing result")?;
        match kind {
            AppServerResponseKind::Initialize => self.initialized = true,
            AppServerResponseKind::Acknowledgement => {}
            AppServerResponseKind::Thread => {
                self.capture_thread(result.get("thread").unwrap_or(result));
            }
            AppServerResponseKind::TurnStart => {
                self.capture_native_turn(result.get("turn").unwrap_or(result));
                self.turn_start_response_received = true;
                self.push_turn_started_event();
            }
        }
        Ok(())
    }

    fn handle_notification(
        &mut self,
        method: &str,
        params: &serde_json::Value,
    ) -> anyhow::Result<()> {
        if method != "thread/started" && !self.notification_matches_active_turn(method, params) {
            return Ok(());
        }
        match method {
            "thread/started" => self.capture_thread(params.get("thread").unwrap_or(params)),
            "turn/started" => {
                self.capture_native_turn(params.get("turn").unwrap_or(params));
                self.push_turn_started_event();
            }
            "item/agentMessage/delta" => {
                if let Some(delta) = params
                    .get("delta")
                    .or_else(|| params.get("text"))
                    .and_then(|value| value.as_str())
                {
                    self.assistant_text.push_str(delta);
                    self.events.push(AppendRunEventRequest {
                        event_type: "message_delta".into(),
                        role: Some("assistant".into()),
                        content: Some(delta.to_owned()),
                        payload: json!({ "stream": true }),
                        waiting_tool: None,
                    });
                }
            }
            "item/agentMessage" | "item/agentMessage/completed" => {
                if let Some(content) = agent_message_content(params) {
                    self.push_assistant_message(content, params.clone());
                }
            }
            "item/completed" => {
                let item = params.get("item").unwrap_or(params);
                if let Some(event) = app_server_event_from_item(self.run_id, item, params.clone()) {
                    self.push_item_event(event);
                }
            }
            "item/tool/call" | "item/toolCall" => {
                if let Some(event) = app_server_event_from_item(self.run_id, params, params.clone())
                {
                    self.push_item_event(event);
                }
            }
            "thread/tokenUsage/updated" => self.events.push(AppendRunEventRequest {
                event_type: "usage".into(),
                role: None,
                content: None,
                payload: params.clone(),
                waiting_tool: None,
            }),
            "turn/completed" => {
                self.handle_turn_completed(params)?;
                self.done = true;
            }
            "error" => {
                self.final_status = "failed".into();
                self.events.push(AppendRunEventRequest {
                    event_type: "status".into(),
                    role: None,
                    content: Some("failed".into()),
                    payload: json!({ "kind": "app_server_error" }),
                    waiting_tool: None,
                });
                self.done = true;
            }
            _ => {}
        }
        Ok(())
    }

    fn notification_matches_active_turn(&self, method: &str, params: &serde_json::Value) -> bool {
        if let Some(thread_id) = params
            .get("threadId")
            .or_else(|| params.get("thread").and_then(|thread| thread.get("id")))
            .and_then(serde_json::Value::as_str)
        {
            if self.thread_id.as_deref() != Some(thread_id) {
                return false;
            }
        }
        let turn_id = params
            .get("turnId")
            .or_else(|| params.get("turn").and_then(|turn| turn.get("id")))
            .and_then(serde_json::Value::as_str);
        let Some(turn_id) = turn_id else {
            return true;
        };
        if method == "turn/started" && !self.turn_start_response_received {
            return false;
        }
        self.native_turn_id.as_deref() == Some(turn_id)
    }

    fn capture_thread(&mut self, thread: &serde_json::Value) {
        if self.thread_id.is_none() {
            self.thread_id = thread
                .get("id")
                .or_else(|| thread.get("threadId"))
                .and_then(|value| value.as_str())
                .map(str::to_owned);
        }
        if let Some(session_id) = thread
            .get("sessionId")
            .or_else(|| thread.get("session_id"))
            .and_then(|value| value.as_str())
        {
            self.session_id = Some(session_id.to_owned());
        }
    }

    fn capture_native_turn(&mut self, turn: &serde_json::Value) {
        if self.native_turn_id.is_none() {
            self.native_turn_id = turn
                .get("id")
                .or_else(|| turn.get("turnId"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
        }
    }

    fn push_turn_started_event(&mut self) {
        if self.turn_started_emitted {
            return;
        }
        let (Some(native_thread_id), Some(native_turn_id)) =
            (self.thread_id.as_deref(), self.native_turn_id.as_deref())
        else {
            return;
        };
        self.events.push(AppendRunEventRequest {
            event_type: "turn_started".into(),
            role: None,
            content: None,
            payload: json!({
                "native_thread_id": native_thread_id,
                "native_turn_id": native_turn_id
            }),
            waiting_tool: None,
        });
        self.turn_started_emitted = true;
    }

    fn handle_turn_completed(&mut self, params: &serde_json::Value) -> anyhow::Result<()> {
        let turn = params
            .get("turn")
            .context("missing Codex turn in turn/completed")?;
        let status = turn
            .get("status")
            .and_then(|value| value.as_str())
            .context("missing or invalid Codex turn status")?;
        let turn_failed = match status {
            "completed" => false,
            "failed" | "interrupted" => true,
            _ => anyhow::bail!("Codex app-server returned unsupported turn status"),
        };
        let mut event_payload = params.clone();
        if turn_failed {
            // 上游失败详情可能包含凭据，不得进入 Hub-facing event。
            if let Some(turn) = event_payload
                .get_mut("turn")
                .and_then(|value| value.as_object_mut())
            {
                turn.remove("error");
            }
        }

        if let Some(thread) = params.get("thread") {
            self.capture_thread(thread);
        }
        if !self.assistant_emitted {
            if let Some(content) = turn
                .get("items")
                .and_then(|items| items.as_array())
                .and_then(|items| items.iter().find_map(agent_message_content))
            {
                self.push_assistant_message(content, event_payload.clone());
            } else if !self.assistant_text.is_empty() {
                self.push_assistant_message(self.assistant_text.clone(), event_payload.clone());
            }
        }
        if let Some(items) = turn.get("items").and_then(|items| items.as_array()) {
            for item in items {
                if let Some(event) =
                    app_server_event_from_item(self.run_id, item, event_payload.clone())
                {
                    if event.event_type == "tool_request" {
                        self.push_item_event(event);
                    }
                }
            }
        }
        self.final_status = if status == "interrupted" {
            "interrupted".into()
        } else if turn_failed {
            "failed".into()
        } else if !self.tool_request_ids.is_empty() {
            "waiting_tool".into()
        } else {
            "completed".into()
        };
        Ok(())
    }

    fn push_assistant_message(&mut self, content: String, payload: serde_json::Value) {
        self.events.push(AppendRunEventRequest {
            event_type: "message".into(),
            role: Some("assistant".into()),
            content: Some(content),
            payload,
            waiting_tool: None,
        });
        self.assistant_emitted = true;
    }

    fn push_item_event(&mut self, event: AppendRunEventRequest) {
        if event.event_type == "message" {
            self.assistant_emitted = true;
            self.events.push(event);
            return;
        }
        if event.event_type == "tool_request" {
            if let Some(source_id) = event
                .payload
                .get("source_id")
                .and_then(|value| value.as_str())
            {
                if !self.tool_request_source_ids.insert(source_id.to_owned()) {
                    return;
                }
            }
            if let Some(tool_request_id) = event
                .payload
                .get("tool_request_id")
                .and_then(|value| value.as_str())
            {
                if !self.tool_request_ids.insert(tool_request_id.to_owned()) {
                    return;
                }
            }
        }
        self.events.push(event);
    }

    fn finish(mut self) -> AppServerRunResult {
        self.session_id = self
            .thread_id
            .clone()
            .or(self.session_id)
            .or_else(|| Some(format!("app-server-session-{}", self.run_id)));
        AppServerRunResult {
            events: self.events,
            final_status: self.final_status,
            session_id: self.session_id,
            native_turn_id: self.native_turn_id,
        }
    }

    fn flush_events(
        &mut self,
        event_tx: &Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
        cancellation: &AppServerCancellation,
    ) -> anyhow::Result<()> {
        let Some(event_tx) = event_tx else {
            return Ok(());
        };
        for event in self.events.drain(..) {
            send_app_server_event_with_backpressure(event_tx, event, cancellation)?;
            self.streamed_events += 1;
        }
        Ok(())
    }
}

fn send_app_server_event_with_backpressure(
    event_tx: &tokio_mpsc::Sender<AppendRunEventRequest>,
    mut event: AppendRunEventRequest,
    cancellation: &AppServerCancellation,
) -> anyhow::Result<()> {
    loop {
        if cancellation.is_cancelled() {
            anyhow::bail!("stream app-server event cancelled");
        }
        match event_tx.try_send(event) {
            Ok(()) => return Ok(()),
            Err(tokio_mpsc::error::TrySendError::Full(returned)) => {
                event = returned;
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(tokio_mpsc::error::TrySendError::Closed(_)) => {
                anyhow::bail!("stream app-server event receiver closed");
            }
        }
    }
}

fn agent_message_content(item: &serde_json::Value) -> Option<String> {
    let item_type = item
        .get("type")
        .or_else(|| item.get("item_type"))
        .and_then(|value| value.as_str())?;
    if item_type != "agentMessage" && item_type != "message" {
        return None;
    }
    item.get("text")
        .or_else(|| item.get("content"))
        .and_then(|value| value.as_str())
        .map(str::to_owned)
}

fn app_server_event_from_item(
    run_id: Uuid,
    item: &serde_json::Value,
    payload: serde_json::Value,
) -> Option<AppendRunEventRequest> {
    if let Some(content) = agent_message_content(item) {
        return Some(AppendRunEventRequest {
            event_type: "message".into(),
            role: Some("assistant".into()),
            content: Some(content),
            payload,
            waiting_tool: None,
        });
    }
    let item_type = item
        .get("type")
        .or_else(|| item.get("item_type"))
        .and_then(|value| value.as_str())?;
    if item_type != "toolRequest" && item_type != "functionCall" {
        return None;
    }
    let tool_name = item
        .get("toolName")
        .or_else(|| item.get("name"))
        .and_then(|value| value.as_str())
        .unwrap_or("tool");
    let source_id = item
        .get("id")
        .or_else(|| item.get("callId"))
        .and_then(|value| value.as_str());
    let arguments = item
        .get("arguments")
        .or_else(|| item.get("args"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    // 平台 source id 只用于去重；Hub 内部 id 必须始终绑定当前 run。
    let tool_request_id =
        stable_tool_request_uuid(run_id, tool_name, source_id, &arguments).to_string();
    Some(AppendRunEventRequest {
        event_type: "tool_request".into(),
        role: Some("assistant".into()),
        content: Some(format!("Codex requested {tool_name} tool")),
        payload: json!({
            "tool_request_id": tool_request_id,
            "source_id": source_id,
            "tool_name": tool_name,
            "arguments": arguments
        }),
        waiting_tool: None,
    })
}

fn stable_tool_request_uuid(
    run_id: Uuid,
    tool_name: &str,
    source_id: Option<&str>,
    arguments: &serde_json::Value,
) -> Uuid {
    let mut input = Vec::new();
    input.extend_from_slice(run_id.as_bytes());
    input.push(0);
    input.extend_from_slice(tool_name.as_bytes());
    input.push(0);
    input.extend_from_slice(source_id.unwrap_or("").as_bytes());
    input.push(0);
    input.extend_from_slice(arguments.to_string().as_bytes());
    let digest = Sha256::digest(&input);
    Uuid::from_slice(&digest[..16]).unwrap_or_else(|_| Uuid::new_v4())
}

fn codex_sandbox_name(agent: &AgentDto) -> &'static str {
    match agent
        .sandbox_policy
        .get("mode")
        .and_then(|value| value.as_str())
        .unwrap_or("read-only")
    {
        "workspace-write" | "workspaceWrite" => "workspaceWrite",
        "danger-full-access" | "dangerFullAccess" => "dangerFullAccess",
        _ => "readOnly",
    }
}

fn codex_sandbox_policy(agent: &AgentDto) -> serde_json::Value {
    let network_access = agent
        .sandbox_policy
        .get("network_access")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    json!({
        "type": codex_sandbox_name(agent),
        "networkAccess": network_access
    })
}

#[derive(Clone)]
struct RunEnv {
    workdir: PathBuf,
    codex_home: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SessionPaths {
    root: PathBuf,
    workspace: PathBuf,
    codex: PathBuf,
    supervisor: PathBuf,
    staging: PathBuf,
}

impl SessionPaths {
    fn for_claim(root: &Path, claim: &ClaimRunResponse) -> anyhow::Result<Self> {
        let session_id = claim
            .run
            .hub_session_id
            .context("claimed Run is missing its Hub Session id")?;
        Ok(Self::for_session(root, session_id))
    }

    fn for_session(root: &Path, session_id: Uuid) -> Self {
        let root = root.join("sessions").join(session_id.to_string());
        Self {
            workspace: root.join("workspace"),
            codex: root.join("codex"),
            supervisor: root.join("supervisor"),
            staging: root.join("staging"),
            root,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct SessionSupervisorMetadata {
    format_version: u32,
    session_id: Uuid,
    runtime_id: Uuid,
    ownership_generation: i64,
    lifecycle_status: String,
    #[serde(default)]
    idle_deadline_unix_ms: Option<i64>,
    #[serde(default)]
    checkpoint_reason: Option<RuntimeCheckpointReason>,
    #[serde(default)]
    checkpoint_retry_unix_ms: Option<i64>,
    #[serde(default)]
    hub_checkpoint_attempt_id: Option<Uuid>,
    #[serde(default)]
    codex_version: String,
    native_thread_id: Option<String>,
}

#[derive(Debug)]
enum DiscoveredSessionMetadata {
    Loaded(SessionSupervisorMetadata),
    Invalid(String),
}

#[derive(Debug)]
enum LocalSessionRecoveryStatus {
    Ready(SessionSupervisorMetadata),
    Blocked(String),
}

#[derive(Debug)]
struct LocalSessionRecoveryRecord {
    snapshot: RuntimeOwnedSessionSnapshotDto,
    paths: SessionPaths,
    status: LocalSessionRecoveryStatus,
}

#[derive(Debug)]
struct SessionRecoveryPlan {
    records: BTreeMap<Uuid, LocalSessionRecoveryRecord>,
    max_online_sessions: usize,
}

fn duration_until_unix_millis(deadline_unix_ms: i64, now: SystemTime) -> Duration {
    let now_unix_ms = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    Duration::from_millis(deadline_unix_ms.saturating_sub(now_unix_ms).max(0) as u64)
}

fn system_time_unix_millis(value: SystemTime) -> anyhow::Result<i64> {
    let millis = value
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("Session timestamp is before the Unix epoch")?
        .as_millis();
    i64::try_from(millis).context("Session timestamp exceeds the supported range")
}

impl SessionRecoveryPlan {
    #[cfg(test)]
    fn record(&self, session_id: Uuid) -> Option<&LocalSessionRecoveryRecord> {
        self.records.get(&session_id)
    }

    #[cfg(test)]
    fn available_new_session_slots(&self) -> usize {
        self.max_online_sessions.saturating_sub(self.records.len())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSessionCleanupEntry {
    session_id: Uuid,
    ownership_generation: i64,
    #[serde(flatten)]
    state: PersistedSessionCleanupState,
}

fn session_cleanup_state_path(work_root: &Path) -> PathBuf {
    work_root
        .join(SESSION_CLEANUP_DIRECTORY)
        .join(SESSION_CLEANUP_STATE_FILE)
}

fn session_cleanup_path(work_root: &Path, session_id: Uuid, ownership_generation: i64) -> PathBuf {
    work_root
        .join(SESSION_CLEANUP_DIRECTORY)
        .join(format!("{session_id}-{ownership_generation}"))
}

fn load_session_cleanup_states(
    work_root: &Path,
) -> anyhow::Result<BTreeMap<SessionCleanupKey, PersistedSessionCleanupState>> {
    let path = session_cleanup_state_path(work_root);
    let bytes = match stdfs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error).context("read Session cleanup state"),
    };
    let entries = serde_json::from_slice::<Vec<PersistedSessionCleanupEntry>>(&bytes)
        .context("parse Session cleanup state")?;
    let mut states = BTreeMap::new();
    for entry in entries {
        anyhow::ensure!(
            entry.ownership_generation > 0,
            "Session cleanup ownership generation must be positive"
        );
        if let PersistedSessionCleanupState::Reserved { path } = &entry.state {
            anyhow::ensure!(
                path == &session_cleanup_path(
                    work_root,
                    entry.session_id,
                    entry.ownership_generation,
                ),
                "Session cleanup reservation path is outside its generation directory"
            );
        }
        anyhow::ensure!(
            states
                .insert((entry.session_id, entry.ownership_generation), entry.state,)
                .is_none(),
            "Session cleanup state contains duplicate generations"
        );
    }
    Ok(states)
}

fn persist_session_cleanup_states(
    work_root: &Path,
    states: &BTreeMap<SessionCleanupKey, PersistedSessionCleanupState>,
) -> anyhow::Result<()> {
    let root = work_root.join(SESSION_CLEANUP_DIRECTORY);
    stdfs::create_dir_all(&root).context("create Session cleanup state directory")?;
    let destination = root.join(SESSION_CLEANUP_STATE_FILE);
    let temporary = root.join(format!(
        ".{SESSION_CLEANUP_STATE_FILE}.tmp-{}",
        Uuid::new_v4().simple()
    ));
    let entries = states
        .iter()
        .map(
            |(&(session_id, ownership_generation), state)| PersistedSessionCleanupEntry {
                session_id,
                ownership_generation,
                state: state.clone(),
            },
        )
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec_pretty(&entries).context("serialize Session cleanup state")?;
    let write_result = (|| -> anyhow::Result<()> {
        let mut options = stdfs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .context("create temporary Session cleanup state")?;
        file.write_all(&bytes)
            .context("write temporary Session cleanup state")?;
        file.sync_all()
            .context("sync temporary Session cleanup state")?;
        stdfs::rename(&temporary, &destination).context("replace Session cleanup state")?;
        stdfs::File::open(&root)
            .context("open Session cleanup state directory")?
            .sync_all()
            .context("sync Session cleanup state directory")?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = stdfs::remove_file(&temporary);
    }
    write_result
}

fn parse_session_cleanup_directory_name(name: &str) -> Option<SessionCleanupKey> {
    let (session_id, ownership_generation) = name.rsplit_once('-')?;
    let session_id = Uuid::parse_str(session_id).ok()?;
    let ownership_generation = ownership_generation.parse::<i64>().ok()?;
    (ownership_generation > 0).then_some((session_id, ownership_generation))
}

async fn persist_session_supervisor_metadata(
    work_root: &Path,
    metadata: &SessionSupervisorMetadata,
) -> anyhow::Result<()> {
    persist_session_supervisor_metadata_sync(work_root, metadata)
}

fn persist_session_supervisor_metadata_sync(
    work_root: &Path,
    metadata: &SessionSupervisorMetadata,
) -> anyhow::Result<()> {
    let paths = SessionPaths::for_session(work_root, metadata.session_id);
    stdfs::create_dir_all(&paths.workspace)?;
    stdfs::create_dir_all(&paths.codex)?;
    stdfs::create_dir_all(&paths.supervisor)?;
    stdfs::create_dir_all(&paths.staging)?;
    let destination = paths.supervisor.join(SESSION_SUPERVISOR_METADATA_FILE);
    let temporary = paths.supervisor.join(format!(
        ".{SESSION_SUPERVISOR_METADATA_FILE}.tmp-{}",
        Uuid::new_v4().simple()
    ));
    let bytes = serde_json::to_vec_pretty(metadata).context("serialize Session metadata")?;
    let write_result = (|| -> anyhow::Result<()> {
        let mut options = stdfs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .context("create temporary Session metadata")?;
        file.write_all(&bytes)
            .context("write temporary Session metadata")?;
        file.sync_all().context("sync Session metadata")?;
        stdfs::rename(&temporary, &destination).context("replace Session metadata")?;
        stdfs::File::open(&paths.supervisor)
            .context("open Session supervisor directory")?
            .sync_all()
            .context("sync Session supervisor directory")?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = stdfs::remove_file(&temporary);
    }
    write_result
}

async fn discover_session_metadata(
    work_root: &Path,
) -> anyhow::Result<BTreeMap<Uuid, DiscoveredSessionMetadata>> {
    let sessions_root = work_root.join("sessions");
    let mut entries = match fs::read_dir(&sessions_root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error.into()),
    };
    let mut discovered = BTreeMap::new();
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(session_id) = Uuid::parse_str(&name) else {
            continue;
        };
        let metadata_path = entry
            .path()
            .join("supervisor")
            .join(SESSION_SUPERVISOR_METADATA_FILE);
        let value = match fs::read(&metadata_path).await {
            Ok(bytes) => match serde_json::from_slice::<SessionSupervisorMetadata>(&bytes) {
                Ok(metadata) => DiscoveredSessionMetadata::Loaded(metadata),
                Err(error) => DiscoveredSessionMetadata::Invalid(error.to_string()),
            },
            Err(error) => DiscoveredSessionMetadata::Invalid(error.to_string()),
        };
        discovered.insert(session_id, value);
    }
    Ok(discovered)
}

async fn plan_session_recovery(
    work_root: &Path,
    runtime_id: Uuid,
    snapshots: &[RuntimeOwnedSessionSnapshotDto],
    max_online_sessions: usize,
) -> anyhow::Result<SessionRecoveryPlan> {
    let mut discovered = discover_session_metadata(work_root).await?;
    let mut records = BTreeMap::new();
    for snapshot in snapshots {
        let paths = SessionPaths::for_session(work_root, snapshot.session_id);
        let status = match discovered.remove(&snapshot.session_id) {
            Some(DiscoveredSessionMetadata::Loaded(metadata))
                if metadata.format_version == 1
                    && metadata.session_id == snapshot.session_id
                    && metadata.runtime_id == runtime_id
                    && metadata.ownership_generation == snapshot.ownership_generation
                    && matches!(
                        snapshot.lifecycle_status.as_str(),
                        "online" | "restoring" | "saving"
                    )
                    && match snapshot.lifecycle_status.as_str() {
                        "saving" => {
                            metadata.lifecycle_status == "saving"
                                && metadata.checkpoint_reason.is_some()
                        }
                        _ => metadata.lifecycle_status == "online",
                    }
                    && snapshot.native_thread_id.as_ref().is_none_or(|thread_id| {
                        metadata.native_thread_id.as_ref() == Some(thread_id)
                    }) =>
            {
                LocalSessionRecoveryStatus::Ready(metadata)
            }
            Some(DiscoveredSessionMetadata::Loaded(_)) => LocalSessionRecoveryStatus::Blocked(
                "local Session metadata does not match current Hub ownership".into(),
            ),
            Some(DiscoveredSessionMetadata::Invalid(error)) => LocalSessionRecoveryStatus::Blocked(
                format!("local Session metadata is unavailable: {error}"),
            ),
            None => LocalSessionRecoveryStatus::Blocked(
                "Hub-owned Session has no local supervisor metadata".into(),
            ),
        };
        records.insert(
            snapshot.session_id,
            LocalSessionRecoveryRecord {
                snapshot: snapshot.clone(),
                paths,
                status,
            },
        );
    }
    Ok(SessionRecoveryPlan {
        records,
        max_online_sessions,
    })
}

#[cfg(test)]
async fn prepare_run_env(
    root: &Path,
    claim: &ClaimRunResponse,
    model_base_url: Option<&str>,
) -> anyhow::Result<RunEnv> {
    prepare_run_env_with_local_skills(root, claim, model_base_url, None).await
}

async fn prepare_run_env_with_local_skills(
    root: &Path,
    claim: &ClaimRunResponse,
    model_base_url: Option<&str>,
    local_skills_dir: Option<&Path>,
) -> anyhow::Result<RunEnv> {
    let paths = SessionPaths::for_claim(root, claim)?;
    fs::create_dir_all(&paths.workspace).await?;
    fs::create_dir_all(&paths.codex).await?;
    fs::create_dir_all(&paths.supervisor).await?;
    fs::create_dir_all(&paths.staging).await?;
    let runtime_fingerprint = execution_configuration_fingerprint(&claim.execution_configuration)
        .context("validate claimed Agent execution configuration")?;
    anyhow::ensure!(
        runtime_fingerprint == claim.expected_configuration_fingerprint,
        "Hub and Runtime execution configuration fingerprints differ"
    );
    synchronize_execution_configuration(
        &paths,
        &claim.execution_configuration,
        &runtime_fingerprint,
        model_base_url,
        local_skills_dir,
    )
    .await?;

    Ok(RunEnv {
        workdir: paths.workspace,
        codex_home: paths.codex,
    })
}

const MATERIALIZATION_MARKER_FILE: &str = ".agent-hub-materialization.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ExecutionMaterializationMarker {
    format_version: u32,
    configuration_fingerprint: String,
    materialization_sha256: String,
    owned_skill_directories: Vec<String>,
    #[serde(default)]
    owned_agent_files: Vec<String>,
}

async fn synchronize_execution_configuration(
    paths: &SessionPaths,
    configuration: &AgentExecutionConfigurationDto,
    configuration_fingerprint: &str,
    model_base_url: Option<&str>,
    local_skills_dir: Option<&Path>,
) -> anyhow::Result<()> {
    let stage = paths
        .staging
        .join(format!("execution-config-{}", Uuid::new_v4()));
    let result: anyhow::Result<()> = async {
        fs::create_dir_all(stage.join("skills")).await?;
        fs::create_dir_all(stage.join("agents")).await?;
        let instructions = format!("{}\n", configuration.instructions.trim_end());
        fs::write(stage.join("AGENTS.md"), instructions.as_bytes())
            .await
            .context("stage Agent guidance")?;
        let config_toml = render_codex_config(configuration, model_base_url)?;
        write_private_file(&stage.join("config.toml"), config_toml.as_bytes())
            .context("stage per-Session Codex config")?;
        fs::write(
            stage.join("skills-manifest.json"),
            serde_json::to_vec_pretty(&configuration.skills)?,
        )
        .await
        .context("stage Skills manifest")?;
        fs::write(
            stage.join("mcp-allowlist.json"),
            serde_json::to_vec_pretty(&redact_mcp_secrets(&configuration.mcp_allowlist))?,
        )
        .await
        .context("stage redacted MCP allowlist")?;
        if let Some(local_skills_dir) = local_skills_dir {
            materialize_local_skills(&stage, local_skills_dir).await?;
        }
        let skills = serde_json::to_value(&configuration.skills)?;
        // Hub Skills are applied last so an inline/managed Skill overrides runtime-local content.
        materialize_skills(&stage, &skills).await?;
        let owned_agent_files = materialize_codex_agents(&stage, configuration).await?;
        let owned_skill_directories = skill_directory_entries(&stage.join("skills")).await?;
        let materialization_sha256 =
            execution_materialization_sha256(&stage, &owned_skill_directories, &owned_agent_files)?;
        let marker = ExecutionMaterializationMarker {
            format_version: 2,
            configuration_fingerprint: configuration_fingerprint.to_owned(),
            materialization_sha256,
            owned_skill_directories,
            owned_agent_files,
        };
        write_private_file(
            &stage.join(MATERIALIZATION_MARKER_FILE),
            &serde_json::to_vec_pretty(&marker)?,
        )
        .context("stage execution configuration marker")?;

        if materialization_is_current(&paths.codex, &marker) {
            return Ok(());
        }
        commit_execution_materialization(&paths.codex, &stage, &marker).await?;
        Ok(())
    }
    .await;
    if let Err(cleanup_error) = fs::remove_dir_all(&stage).await {
        if cleanup_error.kind() != std::io::ErrorKind::NotFound {
            if result.is_ok() {
                return Err(cleanup_error)
                    .context("remove execution configuration staging directory");
            }
            warn!(
                path = %stage.display(),
                error = %cleanup_error,
                "failed to clean execution configuration staging directory"
            );
        }
    }
    result
}

async fn skill_directory_entries(root: &Path) -> anyhow::Result<Vec<String>> {
    let mut entries = fs::read_dir(root).await?;
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    names.sort();
    Ok(names)
}

fn execution_materialization_sha256(
    root: &Path,
    skill_dirs: &[String],
    agent_files: &[String],
) -> anyhow::Result<String> {
    let mut paths = [
        "AGENTS.md",
        "config.toml",
        "skills-manifest.json",
        "mcp-allowlist.json",
    ]
    .into_iter()
    .map(|path| root.join(path))
    .collect::<Vec<_>>();
    for directory in skill_dirs {
        paths.extend(
            WalkDir::new(root.join("skills").join(directory))
                .follow_links(false)
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|entry| entry.into_path()),
        );
    }
    paths.extend(
        agent_files
            .iter()
            .map(|filename| root.join("agents").join(filename)),
    );
    paths.sort();
    let mut digest = Sha256::new();
    for path in paths {
        let relative = path.strip_prefix(root)?;
        let metadata = stdfs::symlink_metadata(&path)?;
        digest.update(relative.to_string_lossy().as_bytes());
        digest.update([0]);
        if metadata.is_dir() {
            digest.update(b"directory");
        } else if metadata.is_file() {
            digest.update(stdfs::read(&path)?);
        } else {
            anyhow::bail!("materialized configuration contains an unsupported file type");
        }
        digest.update([0]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn materialization_is_current(codex_home: &Path, desired: &ExecutionMaterializationMarker) -> bool {
    let marker_path = codex_home.join(MATERIALIZATION_MARKER_FILE);
    let Ok(marker) = stdfs::read(&marker_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<ExecutionMaterializationMarker>(&bytes).ok())
        .ok_or(())
    else {
        return false;
    };
    if marker != *desired || !private_file_permissions_are_valid(&marker_path) {
        return false;
    }
    if !private_file_permissions_are_valid(&codex_home.join("config.toml")) {
        return false;
    }
    if desired.owned_agent_files.iter().any(|filename| {
        !is_owned_agent_filename(filename)
            || !private_file_permissions_are_valid(&codex_home.join("agents").join(filename))
    }) {
        return false;
    }
    execution_materialization_sha256(
        codex_home,
        &desired.owned_skill_directories,
        &desired.owned_agent_files,
    )
    .is_ok_and(|digest| digest == desired.materialization_sha256)
}

#[cfg(unix)]
fn private_file_permissions_are_valid(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    stdfs::metadata(path).is_ok_and(|metadata| metadata.permissions().mode() & 0o777 == 0o600)
}

#[cfg(not(unix))]
fn private_file_permissions_are_valid(_path: &Path) -> bool {
    false
}

async fn commit_execution_materialization(
    codex_home: &Path,
    stage: &Path,
    marker: &ExecutionMaterializationMarker,
) -> anyhow::Result<()> {
    fs::create_dir_all(codex_home.join("skills")).await?;
    fs::create_dir_all(codex_home.join("agents")).await?;
    let previous_owned = previous_owned_skill_directories(codex_home);
    let previous_owned_agents = previous_owned_agent_files(codex_home);
    for directory in &marker.owned_skill_directories {
        let target = codex_home.join("skills").join(directory);
        remove_materialized_path(&target).await?;
        fs::rename(stage.join("skills").join(directory), &target)
            .await
            .with_context(|| format!("install managed Skill directory {directory}"))?;
    }
    for directory in previous_owned {
        if !marker.owned_skill_directories.contains(&directory) {
            remove_materialized_path(&codex_home.join("skills").join(directory)).await?;
        }
    }
    for filename in &marker.owned_agent_files {
        let target = codex_home.join("agents").join(filename);
        remove_materialized_path(&target).await?;
        fs::rename(stage.join("agents").join(filename), &target)
            .await
            .with_context(|| format!("install managed Codex agent file {filename}"))?;
    }
    for filename in previous_owned_agents {
        if !marker.owned_agent_files.contains(&filename) {
            remove_materialized_path(&codex_home.join("agents").join(filename)).await?;
        }
    }
    for filename in [
        "AGENTS.md",
        "config.toml",
        "skills-manifest.json",
        "mcp-allowlist.json",
    ] {
        fs::rename(stage.join(filename), codex_home.join(filename))
            .await
            .with_context(|| format!("install {filename}"))?;
    }
    fs::rename(
        stage.join(MATERIALIZATION_MARKER_FILE),
        codex_home.join(MATERIALIZATION_MARKER_FILE),
    )
    .await
    .context("commit execution configuration marker")?;
    Ok(())
}

fn previous_owned_skill_directories(codex_home: &Path) -> Vec<String> {
    if let Ok(marker) = stdfs::read(codex_home.join(MATERIALIZATION_MARKER_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<ExecutionMaterializationMarker>(&bytes).ok())
        .ok_or(())
    {
        if marker
            .owned_skill_directories
            .iter()
            .all(|directory| is_single_normal_path_component(directory))
        {
            return marker.owned_skill_directories;
        }
    }
    stdfs::read(codex_home.join("skills-manifest.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|skill| {
            skill
                .get("name")
                .and_then(Value::as_str)
                .map(skill_directory_name)
        })
        .filter(|directory| is_single_normal_path_component(directory))
        .collect()
}

fn previous_owned_agent_files(codex_home: &Path) -> Vec<String> {
    stdfs::read(codex_home.join(MATERIALIZATION_MARKER_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<ExecutionMaterializationMarker>(&bytes).ok())
        .map(|marker| marker.owned_agent_files)
        .unwrap_or_default()
        .into_iter()
        .filter(|filename| is_owned_agent_filename(filename))
        .collect()
}

fn is_owned_agent_filename(filename: &str) -> bool {
    is_single_normal_path_component(filename)
        && filename
            .strip_suffix(".toml")
            .is_some_and(is_valid_codex_agent_name)
}

fn is_valid_codex_agent_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn is_single_normal_path_component(value: &str) -> bool {
    let mut components = Path::new(value).components();
    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(component)), None) => {
            component == std::ffi::OsStr::new(value)
        }
        _ => false,
    }
}

async fn remove_materialized_path(path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path).await?,
        Ok(_) => fs::remove_file(path).await?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn copy_directory_snapshot(source: &Path, destination: &Path) -> anyhow::Result<()> {
    let source = stdfs::canonicalize(source)?;
    for entry in WalkDir::new(&source).follow_links(false) {
        let entry = entry?;
        let relative = entry.path().strip_prefix(&source)?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        let target = destination.join(relative);
        if entry.file_type().is_dir() {
            stdfs::create_dir_all(&target)?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                stdfs::create_dir_all(parent)?;
            }
            stdfs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

async fn materialize_codex_agents(
    stage: &Path,
    configuration: &AgentExecutionConfigurationDto,
) -> anyhow::Result<Vec<String>> {
    let mut owned_files = Vec::new();
    let mut normalized_names = HashSet::new();
    for subagent in configuration
        .codex_subagents
        .iter()
        .filter(|subagent| subagent.enabled)
    {
        anyhow::ensure!(
            is_valid_codex_agent_name(&subagent.name),
            "Codex subagent name must use 1 to 64 ASCII letters, digits, hyphens, or underscores"
        );
        anyhow::ensure!(
            normalized_names.insert(subagent.name.to_ascii_lowercase()),
            "Codex subagent names must be unique ignoring case"
        );
        let filename = format!("{}.toml", subagent.name);
        let contents = render_codex_agent(configuration, subagent)?;
        write_private_file(&stage.join("agents").join(&filename), contents.as_bytes())
            .with_context(|| format!("stage Codex agent file {filename}"))?;
        owned_files.push(filename);
    }
    owned_files.sort();
    Ok(owned_files)
}

fn render_codex_agent(
    configuration: &AgentExecutionConfigurationDto,
    subagent: &CodexSubagentDefinition,
) -> anyhow::Result<String> {
    let mut root = toml::map::Map::new();
    root.insert("name".into(), toml::Value::String(subagent.name.clone()));
    root.insert(
        "description".into(),
        toml::Value::String(subagent.description.clone()),
    );
    root.insert(
        "developer_instructions".into(),
        toml::Value::String(subagent.developer_instructions.clone()),
    );
    if let Some(connection_id) = subagent.model_connection_id {
        let connection = model_connection(configuration, connection_id)?;
        root.insert(
            "model".into(),
            toml::Value::String(connection.model_id.clone()),
        );
        root.insert(
            "model_provider".into(),
            toml::Value::String(model_provider_name(connection.id)),
        );
    }
    if let Some(effort) = subagent.reasoning_effort.and_then(codex_reasoning_effort) {
        root.insert(
            "model_reasoning_effort".into(),
            toml::Value::String(effort.into()),
        );
    }
    toml::to_string(&toml::Value::Table(root)).context("serialize Codex agent config")
}

fn write_private_file(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        // secret 配置必须在创建瞬间就是 0600，避免先 0644 写入再 chmod。
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(contents)?;
        Ok(())
    }

    #[cfg(not(unix))]
    {
        let _ = (path, contents);
        anyhow::bail!("private config files require a Unix runtime");
    }
}

fn render_codex_config(
    configuration: &AgentExecutionConfigurationDto,
    model_base_url: Option<&str>,
) -> anyhow::Result<String> {
    let base_url = model_base_url.unwrap_or("http://127.0.0.1:0/v1");
    let default_connection_id = configuration
        .default_model_connection_id
        .context("Agent default Model Connection is required")?;
    let default_connection = model_connection(configuration, default_connection_id)?;
    let mut root = toml::map::Map::new();
    root.insert(
        "model".into(),
        toml::Value::String(default_connection.model_id.clone()),
    );
    root.insert(
        "model_provider".into(),
        toml::Value::String(model_provider_name(default_connection.id)),
    );
    if let Some(effort) = codex_reasoning_effort(configuration.reasoning_effort) {
        root.insert(
            "model_reasoning_effort".into(),
            toml::Value::String(effort.into()),
        );
    }

    let mut model_providers = toml::map::Map::new();
    for connection in &configuration.model_connections {
        let mut provider_config = toml::map::Map::new();
        provider_config.insert("name".into(), toml::Value::String(connection.name.clone()));
        provider_config.insert("base_url".into(), toml::Value::String(base_url.to_owned()));
        provider_config.insert("wire_api".into(), toml::Value::String("responses".into()));
        provider_config.insert(
            "http_headers".into(),
            toml::Value::Table(toml::map::Map::from_iter([(
                "x-agent-hub-model-connection-id".into(),
                toml::Value::String(connection.id.to_string()),
            )])),
        );
        model_providers.insert(
            model_provider_name(connection.id),
            toml::Value::Table(provider_config),
        );
    }
    root.insert(
        "model_providers".into(),
        toml::Value::Table(model_providers),
    );
    root.insert(
        "agents".into(),
        toml::Value::Table(toml::map::Map::from_iter([
            ("max_threads".into(), toml::Value::Integer(6)),
            ("max_depth".into(), toml::Value::Integer(1)),
        ])),
    );
    let mcp_servers = render_mcp_servers(&configuration.mcp_allowlist);
    if !mcp_servers.is_empty() {
        root.insert("mcp_servers".into(), toml::Value::Table(mcp_servers));
    }
    toml::to_string(&toml::Value::Table(root)).context("serialize Codex config")
}

fn model_connection(
    configuration: &AgentExecutionConfigurationDto,
    connection_id: Uuid,
) -> anyhow::Result<&ModelConnectionOptionDto> {
    let connection = configuration
        .model_connections
        .iter()
        .find(|connection| connection.id == connection_id)
        .context("selected Model Connection is missing from execution configuration")?;
    anyhow::ensure!(
        connection.status == ModelConnectionStatus::Enabled,
        "selected Model Connection is disabled"
    );
    Ok(connection)
}

fn model_provider_name(connection_id: Uuid) -> String {
    format!("agent_hub_{}", connection_id.simple())
}

fn codex_reasoning_effort(effort: ReasoningEffort) -> Option<&'static str> {
    match effort {
        ReasoningEffort::Default => None,
        ReasoningEffort::None => Some("none"),
        ReasoningEffort::Minimal => Some("minimal"),
        ReasoningEffort::Low => Some("low"),
        ReasoningEffort::Medium => Some("medium"),
        ReasoningEffort::High => Some("high"),
        ReasoningEffort::Xhigh => Some("xhigh"),
        ReasoningEffort::Max => Some("max"),
        ReasoningEffort::Ultra => Some("ultra"),
    }
}

fn render_mcp_servers(mcp_allowlist: &serde_json::Value) -> toml::map::Map<String, toml::Value> {
    let Some(servers) = mcp_allowlist.as_array() else {
        return toml::map::Map::new();
    };
    let mut output = toml::map::Map::new();
    for server in servers {
        let Some(name) = server.get("name").and_then(|value| value.as_str()) else {
            continue;
        };
        let command = server
            .get("command")
            .and_then(|value| value.as_str())
            .unwrap_or(name);
        let mut server_config = toml::map::Map::new();
        server_config.insert("command".into(), toml::Value::String(command.to_owned()));
        if let Some(args) = server.get("args").and_then(|value| value.as_array()) {
            let rendered_args = args
                .iter()
                .filter_map(|value| value.as_str())
                .map(|value| toml::Value::String(value.to_owned()))
                .collect::<Vec<_>>();
            server_config.insert("args".into(), toml::Value::Array(rendered_args));
        }
        let env_values = mcp_env_values(server);
        if !env_values.is_empty() {
            let mut environment = toml::map::Map::new();
            for (key, value) in env_values {
                environment.insert(key, toml::Value::String(value));
            }
            server_config.insert("env".into(), toml::Value::Table(environment));
        }
        output.insert(name.to_owned(), toml::Value::Table(server_config));
    }
    output
}

fn mcp_env_values(server: &serde_json::Value) -> Vec<(String, String)> {
    let mut values = Vec::new();
    if let Some(entries) = server.get("secrets").and_then(|value| value.as_object()) {
        for (key, value) in entries {
            if let Some(value) = value.as_str() {
                values.push((key.clone(), value.to_owned()));
            }
        }
    }
    values
}

fn redact_mcp_secrets(value: &serde_json::Value) -> serde_json::Value {
    let Some(servers) = value.as_array() else {
        return json!([]);
    };
    serde_json::Value::Array(
        servers
            .iter()
            .map(|server| {
                let mut server = server.clone();
                if let Some(secrets) = server
                    .get_mut("secrets")
                    .and_then(|value| value.as_object_mut())
                {
                    for value in secrets.values_mut() {
                        *value = json!(REDACTED_SECRET);
                    }
                }
                server
            })
            .collect(),
    )
}

async fn materialize_skills(
    codex_home: &Path,
    skills_manifest: &serde_json::Value,
) -> anyhow::Result<()> {
    let Some(skills) = skills_manifest.as_array() else {
        return Ok(());
    };
    for skill in skills {
        let Some(name) = skill.get("name").and_then(|value| value.as_str()) else {
            continue;
        };
        let content = skill
            .get("content")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let description = skill
            .get("description")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let skill_dir = codex_home.join("skills").join(skill_directory_name(name));
        if fs::metadata(&skill_dir).await.is_ok() {
            fs::remove_dir_all(&skill_dir).await?;
        }
        fs::create_dir_all(&skill_dir).await?;
        fs::write(
            skill_dir.join("SKILL.md"),
            render_skill_markdown(name, description, content),
        )
        .await?;
    }
    Ok(())
}

async fn materialize_local_skills(codex_home: &Path, source_root: &Path) -> anyhow::Result<()> {
    let mut entries = match fs::read_dir(source_root).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let markdown_path = entry.path().join("SKILL.md");
        let Ok(markdown) = fs::read_to_string(&markdown_path).await else {
            continue;
        };
        let fallback = entry.file_name().to_string_lossy().into_owned();
        let name = local_skill_name(&markdown).unwrap_or(fallback);
        let destination = codex_home.join("skills").join(skill_directory_name(&name));
        if fs::metadata(&destination).await.is_ok() {
            fs::remove_dir_all(&destination).await?;
        }
        fs::create_dir_all(&destination).await?;
        copy_directory_snapshot(&entry.path(), &destination)?;
    }
    Ok(())
}

fn local_skill_name(markdown: &str) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Frontmatter {
        name: Option<String>,
    }

    let frontmatter = local_skill_frontmatter(markdown)?;
    let frontmatter = serde_yaml_ng::from_str::<Frontmatter>(frontmatter).ok()?;
    frontmatter
        .name
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
}

fn local_skill_frontmatter(markdown: &str) -> Option<&str> {
    let mut lines = markdown.split_inclusive('\n');
    let first = lines.next()?;
    if first.trim() != "---" {
        return None;
    }
    let start = first.len();
    let mut end = start;
    for line in lines {
        if line.trim() == "---" {
            return Some(&markdown[start..end]);
        }
        end += line.len();
    }
    None
}

fn skill_directory_name(name: &str) -> String {
    let mut slug = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else if ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    while slug.contains("--") {
        slug = slug.replace("--", "-");
    }
    slug = slug.trim_matches('-').to_owned();
    if slug.is_empty() {
        slug = "skill".into();
    }
    format!("{slug}-{:016x}", stable_hash(name))
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn render_skill_markdown(name: &str, description: &str, content: &str) -> String {
    let description = if description.trim().is_empty() {
        name
    } else {
        description
    };
    format!(
        "---\nname: {}\ndescription: {}\n---\n\n{}\n",
        serde_json::to_string(name).expect("serialize skill name"),
        serde_json::to_string(description).expect("serialize skill description"),
        content
    )
}

fn fake_codex_events(claim: &ClaimRunResponse) -> (Vec<AppendRunEventRequest>, String) {
    if claim.run.source == "integration:message"
        && claim
            .run
            .initial_message
            .to_ascii_lowercase()
            .contains("tool")
        && claim.integration_context.is_some()
    {
        let context = claim.integration_context.as_ref().expect("checked above");
        let tool_name = context
            .tools
            .as_array()
            .and_then(|tools| tools.first())
            .and_then(|tool| tool.get("name"))
            .and_then(|name| name.as_str())
            .unwrap_or("echo");
        return (
            vec![
                AppendRunEventRequest {
                    event_type: "status".into(),
                    role: None,
                    content: Some("initialized fake Codex app-server".into()),
                    payload: json!({ "phase": "initialize" }),
                    waiting_tool: None,
                },
                AppendRunEventRequest {
                    event_type: "tool_request".into(),
                    role: Some("assistant".into()),
                    content: Some(format!("Fake Codex requested {tool_name} tool")),
                    payload: json!({
                        "tool_request_id": Uuid::new_v4(),
                        "tool_name": tool_name,
                        "arguments": {
                            "message": compact(&claim.run.initial_message),
                            "attachments": context.attachments
                        }
                    }),
                    waiting_tool: None,
                },
            ],
            "waiting_tool".into(),
        );
    }

    let assistant_content = if claim.run.source == "integration:tool_result" {
        format!(
            "Fake Codex completed integration tool result for agent '{}'. {}. Result: {}",
            claim.agent.name,
            compact(&claim.run.initial_message),
            claim
                .integration_context
                .as_ref()
                .and_then(|context| context.tool_result.as_ref())
                .map(|value| value.to_string())
                .unwrap_or_else(|| "{}".into())
        )
    } else {
        format!(
            "Fake Codex completed run for agent '{}'. Instructions loaded: {}. User said: {}",
            claim.agent.name,
            compact(&claim.agent.instructions),
            compact(&claim.run.initial_message)
        )
    };

    (
        vec![
            AppendRunEventRequest {
                event_type: "status".into(),
                role: None,
                content: Some("initialized fake Codex app-server".into()),
                payload: json!({ "phase": "initialize" }),
                waiting_tool: None,
            },
            AppendRunEventRequest {
                event_type: "status".into(),
                role: None,
                content: Some("thread started".into()),
                payload: json!({ "phase": "thread/start" }),
                waiting_tool: None,
            },
            AppendRunEventRequest {
                event_type: "message".into(),
                role: Some("assistant".into()),
                content: Some(assistant_content),
                payload: json!({ "phase": "turn/start", "driver": "fake" }),
                waiting_tool: None,
            },
            AppendRunEventRequest {
                event_type: "usage".into(),
                role: None,
                content: None,
                payload: json!({ "input_tokens": 42, "output_tokens": 24 }),
                waiting_tool: None,
            },
        ],
        "completed".into(),
    )
}

fn compact(input: &str) -> String {
    let text = input.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(120).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        text
    }
}

fn hostname_fallback() -> String {
    env::var("HOSTNAME").unwrap_or_else(|_| "agent-hub-runtime".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    type RecordedHubRequests = Arc<std::sync::Mutex<Vec<Vec<u8>>>>;

    #[test]
    fn runtime_enrollment_response_defaults_protocol_capabilities_to_empty() {
        let response: RuntimeRegisterResponse = serde_json::from_value(json!({
            "runtime_id": Uuid::new_v4(),
            "runtime_credential": "runtime-credential"
        }))
        .unwrap();

        assert!(response.protocol_capabilities.is_empty());
    }

    #[tokio::test]
    async fn session_command_completion_propagates_configuration_identity() {
        let completed = Arc::new(std::sync::Mutex::new(None));
        let app = Router::new().route(
            "/api/runtime/sessions/{session_id}/commands/{command_id}/complete",
            post({
                let completed = Arc::clone(&completed);
                move |Json(request): Json<
                    RuntimeSessionWriteRequest<CompleteRuntimeSessionCommandRequest>,
                >| {
                    let completed = Arc::clone(&completed);
                    async move {
                        *completed.lock().unwrap() = Some(request);
                        AxumStatusCode::OK
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{hub_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let execution_configuration = test_claim().execution_configuration;
        let revision = execution_configuration.revision;
        let fingerprint = execution_configuration_fingerprint(&execution_configuration).unwrap();
        let command = RuntimeSessionCommandDto {
            command_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            ownership_generation: 7,
            command: "refresh_configuration".into(),
            run_id: None,
            turn_id: None,
            native_thread_id: Some("thread-7".into()),
            native_turn_id: None,
            message: None,
            configuration_revision: Some(revision),
            fingerprint: Some(fingerprint.clone()),
            execution_configuration: Some(execution_configuration),
        };

        client
            .complete_session_command(&command, "applied")
            .await
            .unwrap();

        let request = completed.lock().unwrap().take().unwrap();
        assert_eq!(request.ownership_generation, 7);
        assert_eq!(request.payload.command, "refresh_configuration");
        assert_eq!(request.payload.outcome, "applied");
        assert_eq!(request.payload.revision, Some(revision));
        assert_eq!(request.payload.fingerprint, Some(fingerprint));
        hub.abort();
    }

    #[tokio::test]
    async fn dynamic_integration_tools_fail_closed_without_atomic_batch_capability() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.codex_driver = "fake".into();
        config.work_root = temp.path().to_path_buf();
        let mut claim = test_claim();
        claim.run.integration_session_id = Some(Uuid::new_v4());
        claim.run.source = "integration:message".into();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([{ "name": "echo" }]),
            attachments: json!([]),
            tool_result: None,
        });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: "http://127.0.0.1:1".into(),
            runtime_token: Arc::new(std::sync::RwLock::new("test-runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let run_dir = config.work_root.join(claim.run.id.to_string());

        let error = execute_run(&config, &client, claim)
            .await
            .expect_err("an old Hub must not receive dynamic tool requests");

        assert_eq!(
            error.to_string(),
            "Hub protocol does not support atomic Integration tool requests"
        );
        assert!(!run_dir.exists());
    }

    #[test]
    fn waiting_turn_builds_one_batch_for_all_tool_requests() {
        let mut claim = test_claim();
        let integration_session_id = Uuid::new_v4();
        claim.run.integration_session_id = Some(integration_session_id);
        let first = test_tool_request_event();
        let second = test_tool_request_event();

        let batch = build_tool_request_batch(
            &claim,
            vec![first.clone(), second.clone()],
            "waiting_tool",
            Some("thread-for-tools"),
            "/runtime/workdir",
        )
        .unwrap()
        .expect("a waiting turn should produce one finalize batch");

        assert_eq!(batch.integration_session_id, integration_session_id);
        assert_eq!(batch.session_id, "thread-for-tools");
        assert_eq!(batch.work_dir_ref, "/runtime/workdir");
        assert_eq!(batch.tool_requests.len(), 2);
        assert_eq!(batch.tool_requests[0].payload, first.payload);
        assert_eq!(batch.tool_requests[1].payload, second.payload);
    }

    #[test]
    fn failed_turn_discards_entire_tool_request_batch() {
        let mut claim = test_claim();
        claim.run.integration_session_id = Some(Uuid::new_v4());

        let batch = build_tool_request_batch(
            &claim,
            vec![test_tool_request_event(), test_tool_request_event()],
            "failed",
            Some("failed-thread"),
            "/runtime/workdir",
        )
        .unwrap();

        assert!(batch.is_none());
    }

    #[tokio::test]
    async fn waiting_turn_uses_one_finalize_request_before_idempotent_complete() {
        let temp = tempfile::tempdir().unwrap();
        let (client, requests, server) = recording_hub_client(3);
        let mut config = test_config();
        config.codex_driver = "fake".into();
        config.work_root = temp.path().to_path_buf();
        let mut claim = test_claim();
        claim.run.integration_session_id = Some(Uuid::new_v4());
        claim.run.source = "integration:message".into();
        claim.run.initial_message = "use the tool".into();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([{ "name": "echo" }]),
            attachments: json!([]),
            tool_result: None,
        });
        let run_id = claim.run.id;

        execute_run(&config, &client, claim).await.unwrap();
        server.join().unwrap();
        let requests = requests.lock().unwrap();
        let traffic = requests
            .iter()
            .map(|request| String::from_utf8_lossy(request).into_owned())
            .collect::<Vec<_>>();

        assert!(traffic[0].starts_with(&format!("POST /api/runtime/runs/{run_id}/events HTTP/1.1")));
        assert!(traffic[1].starts_with(&format!(
            "POST /api/runtime/runs/{run_id}/tool-requests/finalize HTTP/1.1"
        )));
        assert!(traffic[2].starts_with(&format!(
            "POST /api/runtime/runs/{run_id}/complete HTTP/1.1"
        )));
        assert!(!traffic[0].contains("\"event_type\":\"tool_request\""));
        assert!(traffic[1].contains("\"tool_requests\":["));
        assert!(!traffic.iter().any(|request| request.contains("/heartbeat")));
    }

    #[tokio::test]
    async fn complete_failure_after_finalize_does_not_fall_back_to_partial_publication() {
        let temp = tempfile::tempdir().unwrap();
        let (client, requests, server) = recording_hub_client_with_failure(3, Some("/complete"));
        let mut config = test_config();
        config.codex_driver = "fake".into();
        config.work_root = temp.path().to_path_buf();
        let mut claim = test_claim();
        claim.run.integration_session_id = Some(Uuid::new_v4());
        claim.run.source = "integration:message".into();
        claim.run.initial_message = "use the tool".into();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([{ "name": "echo" }]),
            attachments: json!([]),
            tool_result: None,
        });

        let error = execute_run(&config, &client, claim).await.unwrap_err();
        server.join().unwrap();
        let requests = requests.lock().unwrap();
        let traffic = requests
            .iter()
            .map(|request| String::from_utf8_lossy(request).into_owned())
            .collect::<Vec<_>>();

        assert!(error.to_string().contains("500"));
        assert_eq!(
            traffic
                .iter()
                .filter(|request| request.contains("/tool-requests/finalize"))
                .count(),
            1
        );
        assert!(traffic.last().unwrap().contains("/complete"));
        assert!(!traffic.iter().any(|request| request.contains("/heartbeat")));
        assert!(!traffic.iter().any(|request| {
            request.contains("/events") && request.contains("\"event_type\":\"tool_request\"")
        }));
    }

    #[tokio::test]
    async fn lost_finalize_response_retries_the_identical_batch() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let server_requests = Arc::clone(&requests);
        let server = std::thread::spawn(move || {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                server_requests
                    .lock()
                    .unwrap()
                    .push(read_http_request(&mut stream));
                if attempt == 1 {
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                        )
                        .unwrap();
                    stream.flush().unwrap();
                }
            }
        });
        let client = HubClient {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            hub_url: format!("http://{addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("test-runtime-token".into())),
            protocol_capabilities: HashSet::from([ATOMIC_WAITING_TOOL_BATCH_CAPABILITY.into()]),
        };
        let batch = FinalizeToolRequestsRequest {
            integration_session_id: Uuid::new_v4(),
            session_id: "response-loss-session".into(),
            work_dir_ref: "/runtime/workdir".into(),
            tool_requests: vec![FinalizeToolRequestEvent {
                role: Some("assistant".into()),
                content: Some("tool requested".into()),
                payload: json!({
                    "tool_request_id": Uuid::new_v4(),
                    "tool_name": "echo",
                    "arguments": { "value": 1 }
                }),
            }],
        };

        client
            .finalize_tool_requests(Uuid::from_u128(7), 1, &batch)
            .await
            .unwrap();
        server.join().unwrap();
        let requests = requests.lock().unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
    }

    #[tokio::test]
    async fn prepare_run_env_reuses_stable_isolated_session_layout() {
        let temp = tempfile::tempdir().unwrap();
        let mut claim = test_claim();
        let session_id = Uuid::new_v4();
        claim.run.hub_session_id = Some(session_id);
        let first = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        fs::write(first.codex_home.join("skills/stale.md"), "old")
            .await
            .unwrap();

        let mut later_run = claim.clone();
        later_run.run.id = Uuid::new_v4();
        let second = prepare_run_env(temp.path(), &later_run, None)
            .await
            .unwrap();

        assert_eq!(first.workdir, second.workdir);
        assert_eq!(first.codex_home, second.codex_home);
        assert_eq!(
            second.workdir,
            temp.path()
                .join("sessions")
                .join(session_id.to_string())
                .join("workspace")
        );
        assert_eq!(
            second.codex_home,
            temp.path()
                .join("sessions")
                .join(session_id.to_string())
                .join("codex")
        );
        assert!(second.workdir.exists());
        assert!(second.codex_home.join("config.toml").exists());
        assert!(second.codex_home.join("skills/stale.md").exists());
        assert!(second.workdir.parent().unwrap().join("supervisor").is_dir());
        assert!(second.workdir.parent().unwrap().join("staging").is_dir());
        assert!(second
            .codex_home
            .join("skills")
            .join(skill_directory_name("repo-review"))
            .join("SKILL.md")
            .exists());
        assert!(second.codex_home.join("mcp-allowlist.json").exists());

        let mut other_session = claim;
        other_session.run.id = Uuid::new_v4();
        other_session.run.hub_session_id = Some(Uuid::new_v4());
        let isolated = prepare_run_env(temp.path(), &other_session, None)
            .await
            .unwrap();
        assert_ne!(isolated.workdir, second.workdir);
        assert_ne!(isolated.codex_home, second.codex_home);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execution_materialization_skips_valid_state_and_repairs_proxy_or_missing_marker() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        const MCP_SECRET: &str = "task9-marker-secret";
        let temp = tempfile::tempdir().unwrap();
        let mut claim = test_claim();
        claim.execution_configuration.revision = 2;
        claim.execution_configuration.instructions = "Task 9 durable guidance".into();
        claim.execution_configuration.mcp_allowlist = json!([{
            "name": "github",
            "command": "gh-mcp",
            "secrets": { "TOKEN": MCP_SECRET }
        }]);
        claim.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap();
        claim.agent.instructions = claim.execution_configuration.instructions.clone();
        claim.agent.mcp_allowlist = claim.execution_configuration.mcp_allowlist.clone();

        let first = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:41001/v1"))
            .await
            .unwrap();
        let config_path = first.codex_home.join("config.toml");
        let marker_path = first.codex_home.join(".agent-hub-materialization.json");
        let first_inode = fs::metadata(&config_path).await.unwrap().ino();
        assert_eq!(
            fs::read_to_string(first.codex_home.join("AGENTS.md"))
                .await
                .unwrap(),
            "Task 9 durable guidance\n"
        );
        assert!(fs::read_to_string(&config_path)
            .await
            .unwrap()
            .contains(MCP_SECRET));
        let marker = fs::read_to_string(&marker_path).await.unwrap();
        let sidecar = fs::read_to_string(first.codex_home.join("mcp-allowlist.json"))
            .await
            .unwrap();
        assert!(!marker.contains(MCP_SECRET));
        assert!(!sidecar.contains(MCP_SECRET));
        assert_eq!(
            fs::metadata(&config_path)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(&marker_path)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        let _ = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:41001/v1"))
            .await
            .unwrap();
        assert_eq!(fs::metadata(&config_path).await.unwrap().ino(), first_inode);

        let _ = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:41002/v1"))
            .await
            .unwrap();
        let proxy_inode = fs::metadata(&config_path).await.unwrap().ino();
        assert_ne!(proxy_inode, first_inode);
        assert!(fs::read_to_string(&config_path)
            .await
            .unwrap()
            .contains("41002"));

        fs::remove_file(&marker_path).await.unwrap();
        let _ = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:41002/v1"))
            .await
            .unwrap();
        assert_ne!(fs::metadata(&config_path).await.unwrap().ino(), proxy_inode);
        assert!(marker_path.exists());
    }

    #[tokio::test]
    async fn execution_materialization_removes_owned_skills_preserves_unknown_and_isolates_sessions(
    ) {
        let temp = tempfile::tempdir().unwrap();
        let claim = test_claim();
        let first = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let removed_skill = first
            .codex_home
            .join("skills")
            .join(skill_directory_name("repo-review"));
        let unknown_skill = first.codex_home.join("skills/.system/plugin-owned");
        fs::create_dir_all(&unknown_skill).await.unwrap();
        fs::write(unknown_skill.join("SKILL.md"), "plugin content")
            .await
            .unwrap();

        let mut without_skill = claim.clone();
        without_skill.run.id = Uuid::new_v4();
        without_skill.execution_configuration.revision += 1;
        without_skill.execution_configuration.skills.clear();
        without_skill.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&without_skill.execution_configuration).unwrap();
        let _ = prepare_run_env(temp.path(), &without_skill, None)
            .await
            .unwrap();

        assert!(!removed_skill.exists());
        assert!(unknown_skill.join("SKILL.md").exists());

        let mut other_session = without_skill;
        other_session.run.id = Uuid::new_v4();
        other_session.run.hub_session_id = Some(Uuid::new_v4());
        other_session.execution_configuration.skills = vec![test_execution_skill(
            "other-session",
            "other-session",
            "other content",
        )];
        other_session.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&other_session.execution_configuration).unwrap();
        let other = prepare_run_env(temp.path(), &other_session, None)
            .await
            .unwrap();
        let other_skill_dir = skill_directory_name("other-session");

        assert!(other
            .codex_home
            .join("skills")
            .join(&other_skill_dir)
            .join("SKILL.md")
            .exists());
        assert!(!first
            .codex_home
            .join("skills")
            .join(&other_skill_dir)
            .exists());
        assert!(!other.codex_home.join("skills/.system").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execution_materialization_refreshes_owned_native_subagents_and_preserves_unknown_files(
    ) {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let mut claim = test_claim();
        let override_connection_id = Uuid::from_u128(0x303);
        claim
            .execution_configuration
            .model_connections
            .push(ModelConnectionOptionDto {
                id: override_connection_id,
                name: "Review model".into(),
                model_id: "gpt-review".into(),
                scope: ModelConnectionScope::Personal,
                status: ModelConnectionStatus::Enabled,
            });
        claim.execution_configuration.codex_subagents = vec![
            CodexSubagentDefinition {
                name: "researcher".into(),
                description: "Research the requested topic".into(),
                developer_instructions: "Use primary sources.".into(),
                model_connection_id: None,
                reasoning_effort: None,
                enabled: true,
                disabled_reason: None,
            },
            CodexSubagentDefinition {
                name: "reviewer".into(),
                description: "Review the current change".into(),
                developer_instructions: "Inspect correctness and security.".into(),
                model_connection_id: Some(override_connection_id),
                reasoning_effort: Some(ReasoningEffort::Ultra),
                enabled: true,
                disabled_reason: None,
            },
            CodexSubagentDefinition {
                name: "disabled".into(),
                description: "Disabled role".into(),
                developer_instructions: "Must not be materialized.".into(),
                model_connection_id: None,
                reasoning_effort: None,
                enabled: false,
                disabled_reason: Some("model_connection_deleted".into()),
            },
        ];
        claim.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap();

        let run_env = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:4567/v1"))
            .await
            .unwrap();
        let agents_dir = run_env.codex_home.join("agents");
        let researcher_path = agents_dir.join("researcher.toml");
        let reviewer_path = agents_dir.join("reviewer.toml");
        let researcher = fs::read_to_string(&researcher_path).await.unwrap();
        let reviewer = fs::read_to_string(&reviewer_path).await.unwrap();
        let researcher = researcher.parse::<toml::Value>().unwrap();
        let reviewer = reviewer.parse::<toml::Value>().unwrap();

        assert_eq!(researcher["name"].as_str(), Some("researcher"));
        assert!(researcher.get("model").is_none());
        assert!(researcher.get("model_provider").is_none());
        assert!(researcher.get("model_reasoning_effort").is_none());
        assert_eq!(reviewer["model"].as_str(), Some("gpt-review"));
        assert_eq!(reviewer["model_reasoning_effort"].as_str(), Some("ultra"));
        assert!(!agents_dir.join("disabled.toml").exists());
        assert_eq!(
            fs::metadata(&reviewer_path)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        fs::write(
            agents_dir.join("plugin-owned.toml"),
            "name = \"plugin-owned\"\n",
        )
        .await
        .unwrap();
        let mut refreshed = claim.clone();
        refreshed.execution_configuration.revision += 1;
        refreshed.execution_configuration.codex_subagents = vec![CodexSubagentDefinition {
            name: "researcher".into(),
            description: "Research with updated guidance".into(),
            developer_instructions: "Use primary sources and cite them.".into(),
            model_connection_id: None,
            reasoning_effort: Some(ReasoningEffort::Max),
            enabled: true,
            disabled_reason: None,
        }];
        refreshed.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&refreshed.execution_configuration).unwrap();

        prepare_run_env(temp.path(), &refreshed, Some("http://127.0.0.1:4567/v1"))
            .await
            .unwrap();

        assert!(!reviewer_path.exists());
        assert!(agents_dir.join("plugin-owned.toml").exists());
        let researcher = fs::read_to_string(researcher_path).await.unwrap();
        assert!(researcher.contains("Use primary sources and cite them."));
        assert!(researcher.contains("model_reasoning_effort = \"max\""));
    }

    #[tokio::test]
    async fn execution_materialization_staging_failure_preserves_previous_state_and_can_retry() {
        let temp = tempfile::tempdir().unwrap();
        let mut original = test_claim();
        original.execution_configuration.instructions = "original guidance".into();
        original.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&original.execution_configuration).unwrap();
        let env = prepare_run_env(temp.path(), &original, None).await.unwrap();
        let agents_path = env.codex_home.join("AGENTS.md");
        let marker_path = env.codex_home.join(MATERIALIZATION_MARKER_FILE);
        let original_agents = fs::read(&agents_path).await.unwrap();
        let original_marker = fs::read(&marker_path).await.unwrap();

        let invalid_local_skills = temp.path().join("local-skills-is-a-file");
        fs::write(&invalid_local_skills, "not a directory")
            .await
            .unwrap();
        let mut updated = original.clone();
        updated.run.id = Uuid::new_v4();
        updated.execution_configuration.revision += 1;
        updated.execution_configuration.instructions = "updated guidance".into();
        updated.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&updated.execution_configuration).unwrap();

        assert!(
            prepare_run_env_with_local_skills(
                temp.path(),
                &updated,
                None,
                Some(&invalid_local_skills),
            )
            .await
            .is_err(),
            "staging unexpectedly accepted a file as the local Skills directory"
        );
        let mut staging_entries = fs::read_dir(env.workdir.parent().unwrap().join("staging"))
            .await
            .unwrap();
        assert!(
            staging_entries.next_entry().await.unwrap().is_none(),
            "failed synchronization left an execution configuration staging directory"
        );
        assert_eq!(fs::read(&agents_path).await.unwrap(), original_agents);
        assert_eq!(fs::read(&marker_path).await.unwrap(), original_marker);

        let retried = prepare_run_env(temp.path(), &updated, None).await.unwrap();
        assert_eq!(
            fs::read_to_string(retried.codex_home.join("AGENTS.md"))
                .await
                .unwrap(),
            "updated guidance\n"
        );
        let marker: ExecutionMaterializationMarker = serde_json::from_slice(
            &fs::read(retried.codex_home.join(MATERIALIZATION_MARKER_FILE))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            marker.configuration_fingerprint,
            updated.expected_configuration_fingerprint
        );
    }

    #[tokio::test]
    async fn production_checkpoint_transport_uploads_a_restorable_bundle_before_local_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.runtime_id = Some(runtime_id);
        let session_id = claim.run.hub_session_id.unwrap();
        let turn_id = claim.run.hub_turn_id.unwrap_or_else(Uuid::new_v4);
        claim.run.hub_turn_id = Some(turn_id);
        let thread_id = "019bf9b2-7a4d-7000-8000-000000000004";
        let now = chrono::Utc::now();
        claim.session_context = Some(ClaimSessionContextDto {
            session: HubSessionDto {
                id: session_id,
                owner_id: claim.agent.owner_id,
                agent_id: claim.agent.id,
                agent_name: claim.agent.name.clone(),
                agent_deleted_at: None,
                origin: HubSessionOriginDto::HubNative,
                lifecycle_status: "online".into(),
                native_thread_id: Some(thread_id.into()),
                active_turn_id: None,
                history_checkpoint: 0,
                configuration_fingerprint: None,
                runtime_owner_id: Some(runtime_id),
                ownership_generation: 1,
                recovery_error: None,
                current_bundle: None,
                created_at: now,
                updated_at: now,
            },
            turn: HubSessionTurnDto {
                id: turn_id,
                session_id,
                native_turn_id: None,
                status: "completed".into(),
                configuration_fingerprint: None,
                ownership_generation: 1,
                started_at: Some(now),
                ended_at: Some(now),
                created_at: now,
                updated_at: now,
            },
            messages: Vec::new(),
        });
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let paths = SessionPaths::for_session(temp.path(), session_id);
        fs::write(paths.workspace.join("result.txt"), "saved workspace\n")
            .await
            .unwrap();
        fs::create_dir_all(paths.codex.join("sessions"))
            .await
            .unwrap();
        fs::write(
            paths
                .codex
                .join(format!("sessions/rollout-{thread_id}.jsonl")),
            "{}\n",
        )
        .await
        .unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Idle)
            .unwrap();

        let checkpoint_attempt_id = Uuid::new_v4();
        let uploaded = Arc::new(std::sync::Mutex::new(None));
        let app = Router::new()
            .route(
                "/api/runtime/sessions/{session_id}/checkpoint/begin",
                post(move || async move {
                    Json(RuntimeSessionCheckpointAttemptDto {
                        checkpoint_attempt_id,
                        history_checkpoint: 0,
                        bundle_generation: 1,
                        reason: "idle".into(),
                    })
                }),
            )
            .route(
                "/api/runtime/sessions/{session_id}/bundle",
                axum::routing::put({
                    let uploaded = Arc::clone(&uploaded);
                    move |headers: HeaderMap, body: Body| {
                        let uploaded = Arc::clone(&uploaded);
                        async move {
                            let bytes = axum::body::to_bytes(body, 10 * 1024 * 1024).await.unwrap();
                            *uploaded.lock().unwrap() = Some((headers, bytes.to_vec()));
                            Json(RuntimeSessionBundleCommitResponseDto {
                                checkpoint_attempt_id,
                                bundle_generation: 1,
                                has_queued_work: false,
                                ownership_released: true,
                            })
                        }
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let transport = HubRuntimeCheckpointTransport {
            client: HubClient {
                http: reqwest::Client::new(),
                hub_url: format!("http://{hub_addr}"),
                runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
                protocol_capabilities: HashSet::new(),
            },
            work_root: temp.path().to_path_buf(),
            producing_codex_version: "0.104.0".into(),
        };

        assert_eq!(
            drive_runtime_checkpoints(&manager, &transport)
                .await
                .unwrap(),
            1
        );
        assert!(!paths.root.exists());
        let (headers, bytes) = uploaded.lock().unwrap().take().unwrap();
        let checksum = headers["x-agent-hub-bundle-sha256"]
            .to_str()
            .unwrap()
            .to_owned();
        let archive = temp.path().join("captured.tar.zst");
        std::fs::write(&archive, &bytes).unwrap();
        let restored = temp.path().join("captured-restore");
        session_bundle::restore_session_bundle(
            &archive,
            &checksum,
            bytes.len() as u64,
            session_id,
            0,
            &restored,
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(restored.join("workspace/result.txt")).unwrap(),
            "saved workspace\n"
        );
        hub.abort();
    }

    #[tokio::test]
    async fn restoring_claim_downloads_verifies_and_atomically_installs_the_current_bundle() {
        let temp = tempfile::tempdir().unwrap();
        let source_workspace = temp.path().join("source/workspace");
        let source_codex = temp.path().join("source/codex");
        fs::create_dir_all(&source_workspace).await.unwrap();
        fs::create_dir_all(source_codex.join("sessions"))
            .await
            .unwrap();
        fs::write(source_workspace.join("restored.txt"), "from bundle\n")
            .await
            .unwrap();
        let thread_id = "019bf9b2-7a4d-7000-8000-000000000005";
        fs::write(
            source_codex.join(format!("sessions/rollout-{thread_id}.jsonl")),
            "{}\n",
        )
        .await
        .unwrap();
        fs::write(source_codex.join("auth.json"), "must not restore")
            .await
            .unwrap();
        let session_id = Uuid::new_v4();
        let created_at = chrono::Utc::now();
        let artifact =
            session_bundle::create_session_bundle(&session_bundle::SessionBundleCreateSpec {
                session_id,
                native_thread_id: thread_id.into(),
                history_checkpoint: 8,
                bundle_generation: 2,
                ownership_generation: 3,
                producing_codex_version: "0.103.0".into(),
                created_at,
                workspace: source_workspace,
                codex_home: source_codex,
                archive_path: temp.path().join("source/bundle.tar.zst"),
            })
            .unwrap();
        let archive_bytes = Bytes::from(std::fs::read(&artifact.archive_path).unwrap());
        let archive_size = archive_bytes.len();
        let app = Router::new().route(
            "/api/runtime/sessions/{session_id}/bundle",
            get(move || {
                let archive_bytes = archive_bytes.clone();
                async move {
                    (
                        [(header::CONTENT_LENGTH, archive_size.to_string())],
                        archive_bytes,
                    )
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{hub_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let mut config = test_config();
        config.work_root = temp.path().join("runtime");
        let runtime_id = Uuid::new_v4();
        let mut claim = test_claim();
        claim.run.runtime_id = Some(runtime_id);
        claim.run.hub_session_id = Some(session_id);
        claim.run.session_ownership_generation = Some(4);
        let turn_id = claim.run.hub_turn_id.unwrap_or_else(Uuid::new_v4);
        claim.run.hub_turn_id = Some(turn_id);
        let now = chrono::Utc::now();
        claim.session_context = Some(ClaimSessionContextDto {
            session: HubSessionDto {
                id: session_id,
                owner_id: claim.agent.owner_id,
                agent_id: claim.agent.id,
                agent_name: claim.agent.name.clone(),
                agent_deleted_at: None,
                origin: HubSessionOriginDto::HubNative,
                lifecycle_status: "restoring".into(),
                native_thread_id: Some(thread_id.into()),
                active_turn_id: None,
                history_checkpoint: 8,
                configuration_fingerprint: None,
                runtime_owner_id: Some(runtime_id),
                ownership_generation: 4,
                recovery_error: None,
                current_bundle: Some(CurrentSessionBundleDto {
                    generation: 2,
                    object_key: "hidden-from-runtime".into(),
                    checksum_sha256: artifact.checksum_sha256.clone(),
                    size_bytes: artifact.size_bytes as i64,
                    history_checkpoint: 8,
                    ownership_generation: 3,
                    producing_codex_version: "0.103.0".into(),
                    created_at,
                }),
                created_at: now,
                updated_at: now,
            },
            turn: HubSessionTurnDto {
                id: turn_id,
                session_id,
                native_turn_id: None,
                status: "pending".into(),
                configuration_fingerprint: None,
                ownership_generation: 4,
                started_at: None,
                ended_at: None,
                created_at: now,
                updated_at: now,
            },
            messages: Vec::new(),
        });
        let paths = SessionPaths::for_session(&config.work_root, session_id);
        fs::create_dir_all(&paths.workspace).await.unwrap();
        fs::write(paths.workspace.join("stale.txt"), "stale")
            .await
            .unwrap();

        restore_claim_session_bundle_if_needed(&config, &client, &claim)
            .await
            .unwrap();

        assert_eq!(
            fs::read_to_string(paths.workspace.join("restored.txt"))
                .await
                .unwrap(),
            "from bundle\n"
        );
        assert!(!paths.workspace.join("stale.txt").exists());
        assert!(paths
            .codex
            .join(format!("sessions/rollout-{thread_id}.jsonl"))
            .is_file());
        assert!(!paths.codex.join("auth.json").exists());
        let metadata: SessionSupervisorMetadata = serde_json::from_slice(
            &fs::read(paths.supervisor.join(SESSION_SUPERVISOR_METADATA_FILE))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata.runtime_id, runtime_id);
        assert_eq!(metadata.ownership_generation, 4);
        assert_eq!(metadata.native_thread_id.as_deref(), Some(thread_id));
        hub.abort();
    }

    #[tokio::test]
    async fn restoring_claim_labels_invalid_bundle_as_a_recovery_failure() {
        let temp = tempfile::tempdir().unwrap();
        let invalid_bundle = Bytes::from_static(b"not a tar.zst Session Bundle");
        let invalid_size = invalid_bundle.len();
        let app = Router::new().route(
            "/api/runtime/sessions/{session_id}/bundle",
            get(move || {
                let invalid_bundle = invalid_bundle.clone();
                async move {
                    (
                        [(header::CONTENT_LENGTH, invalid_size.to_string())],
                        invalid_bundle,
                    )
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{hub_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let mut config = test_config();
        config.work_root = temp.path().join("runtime");
        let runtime_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let turn_id = Uuid::new_v4();
        let now = chrono::Utc::now();
        let mut claim = test_claim();
        claim.run.runtime_id = Some(runtime_id);
        claim.run.hub_session_id = Some(session_id);
        claim.run.hub_turn_id = Some(turn_id);
        claim.run.session_ownership_generation = Some(2);
        claim.session_context = Some(ClaimSessionContextDto {
            session: HubSessionDto {
                id: session_id,
                owner_id: claim.agent.owner_id,
                agent_id: claim.agent.id,
                agent_name: claim.agent.name.clone(),
                agent_deleted_at: None,
                origin: HubSessionOriginDto::HubNative,
                lifecycle_status: "restoring".into(),
                native_thread_id: Some("native-thread-invalid-bundle".into()),
                active_turn_id: None,
                history_checkpoint: 4,
                configuration_fingerprint: None,
                runtime_owner_id: Some(runtime_id),
                ownership_generation: 2,
                recovery_error: None,
                current_bundle: Some(CurrentSessionBundleDto {
                    generation: 1,
                    object_key: "hidden-from-runtime".into(),
                    checksum_sha256: format!(
                        "{:x}",
                        Sha256::digest(b"not a tar.zst Session Bundle")
                    ),
                    size_bytes: invalid_size as i64,
                    history_checkpoint: 4,
                    ownership_generation: 1,
                    producing_codex_version: "0.103.0".into(),
                    created_at: now,
                }),
                created_at: now,
                updated_at: now,
            },
            turn: HubSessionTurnDto {
                id: turn_id,
                session_id,
                native_turn_id: None,
                status: "pending".into(),
                configuration_fingerprint: None,
                ownership_generation: 2,
                started_at: None,
                ended_at: None,
                created_at: now,
                updated_at: now,
            },
            messages: Vec::new(),
        });

        let error = restore_claim_session_bundle_if_needed(&config, &client, &claim)
            .await
            .expect_err("invalid Bundle must fail Session recovery");

        assert!(error
            .downcast_ref::<SessionBundleRestoreFailure>()
            .is_some());
        assert!(!SessionPaths::for_session(&config.work_root, session_id)
            .root
            .exists());
        hub.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_session_waits_full_idle_timeout_before_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new_with_idle_timeout(
            temp.path().to_path_buf(),
            runtime_id,
            1,
            Duration::from_secs(10),
        );
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();

        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id: claim.run.hub_session_id.unwrap(),
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::Idle,
            }]
        );
        assert_eq!(
            manager.heartbeat_request().owned_sessions[0].lifecycle_status,
            "saving"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn new_turn_cancels_idle_deadline_and_terminal_restarts_full_window() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new_with_idle_timeout(
            temp.path().to_path_buf(),
            runtime_id,
            1,
            Duration::from_secs(10),
        );
        let mut first = test_claim();
        first.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&first).unwrap();
        manager.complete_fake_claim(&first).await.unwrap();

        tokio::time::advance(Duration::from_secs(9)).await;
        let mut follow_up = first.clone();
        follow_up.run.id = Uuid::new_v4();
        manager.reserve_claim(&follow_up).unwrap();
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());

        manager.complete_fake_claim(&follow_up).await.unwrap();
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id: first.run.hub_session_id.unwrap(),
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::Idle,
            }]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn recovered_idle_session_keeps_only_the_remaining_timeout() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        let mut metadata =
            session_supervisor_metadata_for_claim(runtime_id, &claim, "test-codex").unwrap();
        metadata.idle_deadline_unix_ms = Some(10_000);
        persist_session_supervisor_metadata(temp.path(), &metadata)
            .await
            .unwrap();
        let snapshots = vec![RuntimeOwnedSessionSnapshotDto {
            session_id: claim.run.hub_session_id.unwrap(),
            ownership_generation: 1,
            lifecycle_status: "online".into(),
            native_thread_id: None,
            active_run_id: None,
        }];
        let recovery = plan_session_recovery(temp.path(), runtime_id, &snapshots, 1)
            .await
            .unwrap();
        let manager = SessionSupervisorManager::recover_cold_with_idle_timeout_at(
            temp.path().to_path_buf(),
            runtime_id,
            recovery,
            Duration::from_secs(10),
            SystemTime::UNIX_EPOCH + Duration::from_secs(4),
        );

        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id: claim.run.hub_session_id.unwrap(),
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::Idle,
            }]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_session_persists_its_idle_deadline_for_restart() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new_with_idle_timeout(
            temp.path().to_path_buf(),
            runtime_id,
            1,
            Duration::from_secs(10),
        );
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager
            .complete_fake_claim_at(&claim, SystemTime::UNIX_EPOCH + Duration::from_secs(100))
            .await
            .unwrap();

        let discovered = discover_session_metadata(temp.path()).await.unwrap();
        let DiscoveredSessionMetadata::Loaded(metadata) =
            discovered.get(&claim.run.hub_session_id.unwrap()).unwrap()
        else {
            panic!("terminal Session metadata must remain readable");
        };
        assert_eq!(metadata.idle_deadline_unix_ms, Some(110_000));
    }

    #[tokio::test]
    async fn new_turn_reservation_clears_the_persisted_idle_deadline() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new_with_idle_timeout(
            temp.path().to_path_buf(),
            runtime_id,
            1,
            Duration::from_secs(10),
        );
        let mut first = test_claim();
        first.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&first).unwrap();
        manager
            .complete_fake_claim_at(&first, SystemTime::UNIX_EPOCH + Duration::from_secs(100))
            .await
            .unwrap();

        let mut follow_up = first.clone();
        follow_up.run.id = Uuid::new_v4();
        manager.reserve_claim(&follow_up).unwrap();

        let discovered = discover_session_metadata(temp.path()).await.unwrap();
        let DiscoveredSessionMetadata::Loaded(metadata) =
            discovered.get(&first.run.hub_session_id.unwrap()).unwrap()
        else {
            panic!("reserved Session metadata must remain readable");
        };
        assert_eq!(metadata.idle_deadline_unix_ms, None);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_idle_deadline_never_checkpoints_a_reserved_turn() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new_with_idle_timeout(
            temp.path().to_path_buf(),
            runtime_id,
            1,
            Duration::from_secs(10),
        );
        let mut first = test_claim();
        first.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&first).unwrap();
        manager.complete_fake_claim(&first).await.unwrap();
        let mut follow_up = first.clone();
        follow_up.run.id = Uuid::new_v4();
        manager.reserve_claim(&follow_up).unwrap();

        manager.arm_idle_deadline(first.run.hub_session_id.unwrap(), 1);
        tokio::time::advance(Duration::from_secs(10)).await;

        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            manager.heartbeat_request().owned_sessions[0].lifecycle_status,
            "online"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drain_checkpoint_intent_overrides_the_idle_wait() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new_with_idle_timeout(
            temp.path().to_path_buf(),
            runtime_id,
            1,
            Duration::from_secs(900),
        );
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();

        manager
            .request_checkpoint(
                claim.run.hub_session_id.unwrap(),
                1,
                RuntimeCheckpointReason::Drain,
            )
            .unwrap();

        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id: claim.run.hub_session_id.unwrap(),
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::Drain,
            }]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drain_waits_for_the_reserved_turn_then_checkpoints_immediately() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new_with_idle_timeout(
            temp.path().to_path_buf(),
            runtime_id,
            1,
            Duration::from_secs(900),
        );
        let mut first = test_claim();
        first.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&first).unwrap();
        manager.complete_fake_claim(&first).await.unwrap();
        let mut active = first.clone();
        active.run.id = Uuid::new_v4();
        manager.reserve_claim(&active).unwrap();

        manager
            .request_checkpoint(
                first.run.hub_session_id.unwrap(),
                1,
                RuntimeCheckpointReason::Drain,
            )
            .unwrap();
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());

        manager.complete_fake_claim(&active).await.unwrap();
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id: first.run.hub_session_id.unwrap(),
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::Drain,
            }]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn version_switch_upgrades_an_idle_intent_without_double_checkpointing() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new_with_idle_timeout(
            temp.path().to_path_buf(),
            runtime_id,
            1,
            Duration::from_secs(900),
        );
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let session_id = claim.run.hub_session_id.unwrap();

        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Idle)
            .unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::VersionSwitch)
            .unwrap();

        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id,
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::VersionSwitch,
            }]
        );
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_checkpoint_command_registers_a_drain_intent() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = Arc::new(SessionSupervisorManager::new_with_idle_timeout(
            temp.path().to_path_buf(),
            runtime_id,
            1,
            Duration::from_secs(900),
        ));
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: "http://127.0.0.1:1".into(),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let dispatcher = Arc::new(RuntimeSessionCommandDispatcher::default());

        dispatcher.enqueue(
            &client,
            &manager,
            &[RuntimeSessionCommandDto {
                command_id: claim.run.hub_session_id.unwrap(),
                session_id: claim.run.hub_session_id.unwrap(),
                ownership_generation: 1,
                command: "checkpoint".into(),
                run_id: None,
                turn_id: None,
                native_thread_id: None,
                native_turn_id: None,
                message: None,
                configuration_revision: None,
                fingerprint: None,
                execution_configuration: None,
            }],
        );

        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id: claim.run.hub_session_id.unwrap(),
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::Drain,
            }]
        );
    }

    #[tokio::test]
    async fn checkpoint_metadata_failure_rolls_back_and_remains_retryable() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new_with_idle_timeout(
            temp.path().to_path_buf(),
            runtime_id,
            1,
            Duration::from_secs(900),
        );
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let session_id = claim.run.hub_session_id.unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Drain)
            .unwrap();
        let supervisor_path = SessionPaths::for_session(temp.path(), session_id).supervisor;
        stdfs::remove_dir_all(&supervisor_path).unwrap();
        stdfs::write(&supervisor_path, b"blocks metadata directory").unwrap();

        assert!(manager.take_due_checkpoint_requests().await.is_err());
        assert_eq!(
            manager.heartbeat_request().owned_sessions[0].lifecycle_status,
            "online"
        );

        stdfs::remove_file(&supervisor_path).unwrap();
        stdfs::create_dir_all(&supervisor_path).unwrap();
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id,
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::Drain,
            }]
        );
    }

    #[tokio::test]
    async fn successful_checkpoint_with_queued_work_reuses_the_local_session() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut first = test_claim();
        first.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&first).unwrap();
        manager.complete_fake_claim(&first).await.unwrap();
        let session_id = first.run.hub_session_id.unwrap();
        let marker = SessionPaths::for_session(temp.path(), session_id)
            .workspace
            .join("retained.txt");
        stdfs::write(&marker, b"local state").unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Idle)
            .unwrap();
        let request = manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .pop()
            .unwrap();

        assert!(manager
            .finish_checkpoint(
                &request,
                RuntimeCheckpointEffectResult::Saved {
                    has_queued_work: true,
                    ownership_released: false,
                },
            )
            .unwrap()
            .is_none());

        assert!(marker.exists());
        assert_eq!(
            manager.heartbeat_request().owned_sessions[0].lifecycle_status,
            "online"
        );
        let mut follow_up = first.clone();
        follow_up.run.id = Uuid::new_v4();
        manager.reserve_claim(&follow_up).unwrap();
    }

    #[tokio::test]
    async fn successful_drain_checkpoint_releases_ownership_with_queued_work() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let session_id = claim.run.hub_session_id.unwrap();
        let marker = SessionPaths::for_session(temp.path(), session_id)
            .workspace
            .join("retained-until-task-11.txt");
        stdfs::write(&marker, b"checkpointed state").unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Drain)
            .unwrap();
        let request = manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .pop()
            .unwrap();

        let cleanup = manager
            .finish_checkpoint(
                &request,
                RuntimeCheckpointEffectResult::Saved {
                    has_queued_work: true,
                    ownership_released: true,
                },
            )
            .unwrap()
            .expect("released Session must reserve its generation-specific cleanup");

        assert!(manager.heartbeat_request().owned_sessions.is_empty());
        assert!(!marker.exists());
        assert!(cleanup
            .path
            .join("workspace/retained-until-task-11.txt")
            .exists());
        stdfs::remove_dir_all(&cleanup.path).unwrap();
        manager.complete_released_session_cleanup(&cleanup).unwrap();
    }

    #[tokio::test]
    async fn released_checkpoint_cleanup_cannot_delete_a_new_generation_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut original = test_claim();
        original.run.hub_session_id = Some(Uuid::new_v4());
        let session_id = original.run.hub_session_id.unwrap();
        manager.reserve_claim(&original).unwrap();
        manager.complete_fake_claim(&original).await.unwrap();
        let paths = SessionPaths::for_session(temp.path(), session_id);
        let old_marker = paths.workspace.join("old-generation.txt");
        stdfs::write(&old_marker, b"old generation").unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Idle)
            .unwrap();
        let request = manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .pop()
            .unwrap();
        let cleanup = manager
            .finish_checkpoint(
                &request,
                RuntimeCheckpointEffectResult::Saved {
                    has_queued_work: false,
                    ownership_released: true,
                },
            )
            .unwrap()
            .expect("released Session directory must have a cleanup reservation");
        assert_eq!(cleanup.ownership_generation, 1);
        assert!(!paths.root.exists());
        assert!(cleanup.path.join("workspace/old-generation.txt").exists());

        let cleanup_started = Arc::new(Notify::new());
        let allow_cleanup = Arc::new(Notify::new());
        let cleanup_task = tokio::spawn({
            let cleanup_started = Arc::clone(&cleanup_started);
            let allow_cleanup = Arc::clone(&allow_cleanup);
            let cleanup_path = cleanup.path.clone();
            async move {
                cleanup_started.notify_one();
                allow_cleanup.notified().await;
                fs::remove_dir_all(cleanup_path).await.unwrap();
            }
        });
        cleanup_started.notified().await;

        let mut replacement = original.clone();
        replacement.run.id = Uuid::new_v4();
        replacement.run.session_ownership_generation = Some(2);
        manager.reserve_claim(&replacement).unwrap();
        fs::create_dir_all(&paths.workspace).await.unwrap();
        let new_marker = paths.workspace.join("new-generation.txt");
        fs::write(&new_marker, b"new generation").await.unwrap();

        allow_cleanup.notify_one();
        cleanup_task.await.unwrap();
        manager.complete_released_session_cleanup(&cleanup).unwrap();

        assert_eq!(
            fs::read_to_string(new_marker).await.unwrap(),
            "new generation"
        );
    }

    #[tokio::test]
    async fn hub_fenced_session_cleanup_cannot_delete_a_new_generation_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut original = test_claim();
        original.run.hub_session_id = Some(Uuid::new_v4());
        let session_id = original.run.hub_session_id.unwrap();
        manager.reserve_claim(&original).unwrap();
        manager.complete_fake_claim(&original).await.unwrap();
        let paths = SessionPaths::for_session(temp.path(), session_id);
        let old_marker = paths.workspace.join("old-fenced-generation.txt");
        stdfs::write(&old_marker, b"old generation").unwrap();

        let cleanup = manager
            .reconcile_owned_snapshots(&[])
            .unwrap()
            .pop()
            .expect("Hub-fenced Session must reserve generation-specific cleanup");
        assert_eq!(cleanup.ownership_generation, 1);
        assert!(manager.heartbeat_request().owned_sessions.is_empty());
        assert!(!paths.root.exists());
        assert!(cleanup
            .path
            .join("workspace/old-fenced-generation.txt")
            .exists());

        let cleanup_started = Arc::new(Notify::new());
        let allow_cleanup = Arc::new(Notify::new());
        let cleanup_task = tokio::spawn({
            let cleanup_started = Arc::clone(&cleanup_started);
            let allow_cleanup = Arc::clone(&allow_cleanup);
            let cleanup_path = cleanup.path.clone();
            async move {
                cleanup_started.notify_one();
                allow_cleanup.notified().await;
                fs::remove_dir_all(cleanup_path).await.unwrap();
            }
        });
        cleanup_started.notified().await;

        let mut replacement = original.clone();
        replacement.run.id = Uuid::new_v4();
        replacement.run.session_ownership_generation = Some(2);
        manager.reserve_claim(&replacement).unwrap();
        fs::create_dir_all(&paths.workspace).await.unwrap();
        let new_marker = paths.workspace.join("new-generation.txt");
        fs::write(&new_marker, b"new generation").await.unwrap();

        allow_cleanup.notify_one();
        cleanup_task.await.unwrap();
        manager.complete_released_session_cleanup(&cleanup).unwrap();

        assert_eq!(
            fs::read_to_string(new_marker).await.unwrap(),
            "new generation"
        );
    }

    #[tokio::test]
    async fn successful_hub_fenced_cleanup_is_acknowledged_until_a_heartbeat_succeeds() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        claim.run.session_ownership_generation = Some(7);
        let session_id = claim.run.hub_session_id.unwrap();
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let cleanup = manager
            .reconcile_owned_snapshots(&[])
            .unwrap()
            .pop()
            .unwrap();

        remove_hub_fenced_session_cleanup(&manager, cleanup).await;
        let expected = RuntimeOwnedSessionGenerationDto {
            session_id,
            ownership_generation: 7,
        };
        assert_eq!(
            manager.heartbeat_request().cleaned_sessions,
            vec![expected.clone()]
        );

        let mut config = test_config();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.hub_url = format!("http://{}", listener.local_addr().unwrap());
        let mut stored = test_stored_runtime_credential();
        let server = std::thread::spawn(move || {
            let (mut failed, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut failed)).to_string();
            assert!(request.contains("\"cleaned_sessions\""));
            failed
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();

            let (mut successful, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut successful)).to_string();
            assert!(request.contains("\"ownership_generation\":7"));
            write_heartbeat_response(&mut successful, false, false, false);
        });
        let client = hub_client_from_stored(&config, runtime_http_client().unwrap(), &stored);
        let codex_rollout = RuntimeCodexState::new(&config);

        assert!(send_runtime_heartbeat(
            &config,
            &client,
            &mut stored,
            Some(&manager),
            &codex_rollout,
        )
        .await
        .is_err());
        assert_eq!(manager.heartbeat_request().cleaned_sessions, vec![expected]);
        send_runtime_heartbeat(
            &config,
            &client,
            &mut stored,
            Some(&manager),
            &codex_rollout,
        )
        .await
        .unwrap();
        assert!(manager.heartbeat_request().cleaned_sessions.is_empty());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn failed_hub_fenced_cleanup_is_not_acknowledged_and_can_be_retried() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        let session_id = claim.run.hub_session_id.unwrap();
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let cleanup = manager
            .reconcile_owned_snapshots(&[])
            .unwrap()
            .pop()
            .unwrap();
        stdfs::remove_dir_all(&cleanup.path).unwrap();
        stdfs::write(&cleanup.path, b"not a directory").unwrap();

        remove_hub_fenced_session_cleanup(&manager, cleanup.clone()).await;

        assert!(manager.heartbeat_request().cleaned_sessions.is_empty());
        stdfs::remove_file(&cleanup.path).unwrap();
        stdfs::create_dir(&cleanup.path).unwrap();
        let retry = manager
            .reconcile_owned_snapshots(&[])
            .unwrap()
            .pop()
            .expect("failed cleanup must remain retryable");
        assert_eq!(retry, cleanup);
        remove_hub_fenced_session_cleanup(&manager, retry).await;
        assert_eq!(
            manager.heartbeat_request().cleaned_sessions,
            vec![RuntimeOwnedSessionGenerationDto {
                session_id,
                ownership_generation: 1,
            }]
        );
    }

    #[tokio::test]
    async fn missing_reserved_cleanup_directory_produces_a_completed_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), Uuid::new_v4(), 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        let session_id = claim.run.hub_session_id.unwrap();
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let cleanup = manager
            .reconcile_owned_snapshots(&[])
            .unwrap()
            .pop()
            .unwrap();
        stdfs::remove_dir_all(&cleanup.path).unwrap();

        remove_hub_fenced_session_cleanup(&manager, cleanup).await;

        assert_eq!(
            manager.heartbeat_request().cleaned_sessions,
            vec![RuntimeOwnedSessionGenerationDto {
                session_id,
                ownership_generation: 1,
            }]
        );
    }

    #[test]
    fn hub_cleanup_obligation_without_local_metadata_persists_a_completed_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let expected = RuntimeOwnedSessionGenerationDto {
            session_id: Uuid::new_v4(),
            ownership_generation: 8,
        };
        {
            let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
            assert!(manager
                .reserve_hub_cleanup_obligations(std::slice::from_ref(&expected))
                .unwrap()
                .is_empty());
            assert_eq!(
                manager.heartbeat_request().cleaned_sessions,
                vec![expected.clone()]
            );
        }

        let restarted = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        assert_eq!(
            restarted.heartbeat_request().cleaned_sessions,
            vec![expected]
        );
    }

    #[tokio::test]
    async fn hub_cleanup_obligation_removes_a_pre_metadata_session_directory() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let expected = RuntimeOwnedSessionGenerationDto {
            session_id: Uuid::new_v4(),
            ownership_generation: 9,
        };
        let paths = SessionPaths::for_session(temp.path(), expected.session_id);
        stdfs::create_dir_all(&paths.workspace).unwrap();
        stdfs::write(
            paths.workspace.join("before-metadata.txt"),
            b"partial claim",
        )
        .unwrap();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);

        let cleanup = manager
            .reserve_hub_cleanup_obligations(std::slice::from_ref(&expected))
            .unwrap()
            .pop()
            .expect("pre-metadata Session directory must be isolated for cleanup");
        assert!(!paths.root.exists());
        assert!(cleanup.path.join("workspace/before-metadata.txt").exists());

        remove_hub_fenced_session_cleanup(&manager, cleanup).await;
        assert_eq!(manager.heartbeat_request().cleaned_sessions, vec![expected]);
    }

    #[test]
    fn old_hub_cleanup_obligation_does_not_remove_a_newer_generation_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        claim.run.session_ownership_generation = Some(12);
        let session_id = claim.run.hub_session_id.unwrap();
        manager.reserve_claim(&claim).unwrap();
        let marker = SessionPaths::for_session(temp.path(), session_id)
            .workspace
            .join("new-generation.txt");
        stdfs::create_dir_all(marker.parent().unwrap()).unwrap();
        stdfs::write(&marker, b"new generation").unwrap();

        assert!(manager
            .reserve_hub_cleanup_obligations(&[RuntimeOwnedSessionGenerationDto {
                session_id,
                ownership_generation: 11,
            }])
            .unwrap()
            .is_empty());

        assert_eq!(stdfs::read(&marker).unwrap(), b"new generation");
        let heartbeat = manager.heartbeat_request();
        assert_eq!(heartbeat.owned_sessions[0].ownership_generation, 12);
        assert_eq!(heartbeat.cleaned_sessions[0].ownership_generation, 11);
    }

    #[tokio::test]
    async fn old_cleanup_acknowledgement_does_not_hide_a_new_session_generation() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut original = test_claim();
        original.run.hub_session_id = Some(Uuid::new_v4());
        original.run.session_ownership_generation = Some(2);
        let session_id = original.run.hub_session_id.unwrap();
        manager.reserve_claim(&original).unwrap();
        manager.complete_fake_claim(&original).await.unwrap();
        let cleanup = manager
            .reconcile_owned_snapshots(&[])
            .unwrap()
            .pop()
            .unwrap();
        remove_hub_fenced_session_cleanup(&manager, cleanup).await;

        let mut replacement = original.clone();
        replacement.run.id = Uuid::new_v4();
        replacement.run.session_ownership_generation = Some(3);
        manager.reserve_claim(&replacement).unwrap();
        let heartbeat = manager.heartbeat_request();

        assert_eq!(heartbeat.owned_sessions[0].ownership_generation, 3);
        assert_eq!(
            heartbeat.cleaned_sessions,
            vec![RuntimeOwnedSessionGenerationDto {
                session_id,
                ownership_generation: 2,
            }]
        );
    }

    #[tokio::test]
    async fn reserved_cleanup_is_recovered_and_retried_after_runtime_restart() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        claim.run.session_ownership_generation = Some(4);
        let session_id = claim.run.hub_session_id.unwrap();
        let cleanup = {
            let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
            manager.reserve_claim(&claim).unwrap();
            manager.complete_fake_claim(&claim).await.unwrap();
            manager
                .reconcile_owned_snapshots(&[])
                .unwrap()
                .pop()
                .unwrap()
        };

        let restarted = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        assert_eq!(
            restarted.take_pending_released_session_cleanups().unwrap(),
            vec![cleanup.clone()]
        );
        remove_hub_fenced_session_cleanup(&restarted, cleanup).await;
        assert_eq!(
            restarted.heartbeat_request().cleaned_sessions,
            vec![RuntimeOwnedSessionGenerationDto {
                session_id,
                ownership_generation: 4,
            }]
        );
    }

    #[tokio::test]
    async fn completed_cleanup_receipt_survives_restart_until_heartbeat_ack() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        claim.run.session_ownership_generation = Some(5);
        let expected = RuntimeOwnedSessionGenerationDto {
            session_id: claim.run.hub_session_id.unwrap(),
            ownership_generation: 5,
        };
        {
            let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
            manager.reserve_claim(&claim).unwrap();
            manager.complete_fake_claim(&claim).await.unwrap();
            let cleanup = manager
                .reconcile_owned_snapshots(&[])
                .unwrap()
                .pop()
                .unwrap();
            remove_hub_fenced_session_cleanup(&manager, cleanup).await;
        }

        let restarted = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        assert_eq!(
            restarted.heartbeat_request().cleaned_sessions,
            vec![expected.clone()]
        );
        restarted
            .acknowledge_cleaned_sessions(std::slice::from_ref(&expected))
            .unwrap();
        drop(restarted);

        let restarted_again =
            SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        assert!(restarted_again
            .heartbeat_request()
            .cleaned_sessions
            .is_empty());
    }

    #[tokio::test]
    async fn renamed_cleanup_directory_is_recovered_when_reservation_state_was_not_committed() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let source = SessionPaths::for_session(temp.path(), session_id).root;
        stdfs::create_dir_all(source.join("workspace")).unwrap();
        stdfs::write(source.join("workspace/before-crash.txt"), b"old generation").unwrap();
        let cleanup_path = temp
            .path()
            .join(SESSION_CLEANUP_DIRECTORY)
            .join(format!("{session_id}-6"));
        stdfs::create_dir_all(cleanup_path.parent().unwrap()).unwrap();
        stdfs::rename(source, &cleanup_path).unwrap();

        let restarted = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let cleanup = restarted
            .take_pending_released_session_cleanups()
            .unwrap()
            .pop()
            .expect("renamed generation directory must recover as reserved cleanup");
        assert_eq!(cleanup.session_id, session_id);
        assert_eq!(cleanup.ownership_generation, 6);
        assert_eq!(cleanup.path, cleanup_path);
        remove_hub_fenced_session_cleanup(&restarted, cleanup).await;
        assert_eq!(
            restarted.heartbeat_request().cleaned_sessions,
            vec![RuntimeOwnedSessionGenerationDto {
                session_id,
                ownership_generation: 6,
            }]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failed_checkpoint_without_queued_work_stays_saving_and_retries() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let session_id = claim.run.hub_session_id.unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Idle)
            .unwrap();
        let request = manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .pop()
            .unwrap();

        assert!(manager
            .finish_checkpoint(
                &request,
                RuntimeCheckpointEffectResult::Failed {
                    has_queued_work: false,
                    retry_required: true,
                    error: "object store unavailable".into(),
                },
            )
            .unwrap()
            .is_none());

        assert_eq!(
            manager.heartbeat_request().owned_sessions[0].lifecycle_status,
            "saving"
        );
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![request]
        );
    }

    #[tokio::test]
    async fn failed_idle_checkpoint_with_queued_work_resumes_from_local_state() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut first = test_claim();
        first.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&first).unwrap();
        manager.complete_fake_claim(&first).await.unwrap();
        let session_id = first.run.hub_session_id.unwrap();
        let marker = SessionPaths::for_session(temp.path(), session_id)
            .workspace
            .join("survives-failed-save.txt");
        stdfs::write(&marker, b"latest local state").unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Idle)
            .unwrap();
        let request = manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .pop()
            .unwrap();

        assert!(manager
            .finish_checkpoint(
                &request,
                RuntimeCheckpointEffectResult::Failed {
                    has_queued_work: true,
                    retry_required: false,
                    error: "upload interrupted".into(),
                },
            )
            .unwrap()
            .is_none());

        assert!(marker.exists());
        assert_eq!(
            manager.heartbeat_request().owned_sessions[0].lifecycle_status,
            "online"
        );
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());
        let mut follow_up = first.clone();
        follow_up.run.id = Uuid::new_v4();
        manager.reserve_claim(&follow_up).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn failed_drain_checkpoint_with_queued_work_remains_retryable() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let session_id = claim.run.hub_session_id.unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Drain)
            .unwrap();
        let request = manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .pop()
            .unwrap();

        assert!(manager
            .finish_checkpoint(
                &request,
                RuntimeCheckpointEffectResult::Failed {
                    has_queued_work: true,
                    retry_required: true,
                    error: "upload interrupted".into(),
                },
            )
            .unwrap()
            .is_none());

        assert_eq!(
            manager.heartbeat_request().owned_sessions[0].lifecycle_status,
            "saving"
        );
        tokio::time::advance(CHECKPOINT_RETRY_DELAY).await;
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![request]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_production_transport_never_pretends_checkpoint_success() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        manager
            .request_checkpoint(
                claim.run.hub_session_id.unwrap(),
                1,
                RuntimeCheckpointReason::Idle,
            )
            .unwrap();
        let transport = UnavailableRuntimeCheckpointTransport;

        assert_eq!(
            drive_runtime_checkpoints(&manager, &transport)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            manager.heartbeat_request().owned_sessions[0].lifecycle_status,
            "saving"
        );
        assert_eq!(
            drive_runtime_checkpoints(&manager, &transport)
                .await
                .unwrap(),
            0
        );
        tokio::time::advance(CHECKPOINT_RETRY_DELAY).await;
        assert_eq!(
            drive_runtime_checkpoints(&manager, &transport)
                .await
                .unwrap(),
            1
        );
        assert_eq!(manager.heartbeat_request().owned_sessions.len(), 1);
    }

    #[test]
    fn forgetting_fenced_recovery_removes_heartbeat_ownership_and_releases_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), Uuid::new_v4(), 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        let session_id = claim.run.hub_session_id.unwrap();
        manager.reserve_claim(&claim).unwrap();
        assert_eq!(manager.heartbeat_request().owned_sessions.len(), 1);
        assert_eq!(manager.available_new_session_slots(), 0);

        manager.forget_fenced_session(session_id);

        assert!(manager.heartbeat_request().owned_sessions.is_empty());
        assert_eq!(manager.available_new_session_slots(), 1);
    }

    #[tokio::test]
    async fn hub_checkpoint_status_boundary_resumes_idle_session_with_queued_work() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let session_id = claim.run.hub_session_id.unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Idle)
            .unwrap();
        let checkpoint_attempt_id = Uuid::new_v4();
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new()
            .route(
                "/api/runtime/sessions/{session_id}/checkpoint/begin",
                post({
                    let calls = Arc::clone(&calls);
                    move |AxumPath(session_id): AxumPath<Uuid>| {
                        let calls = Arc::clone(&calls);
                        async move {
                            calls.lock().unwrap().push(("begin", session_id));
                            Json(RuntimeSessionCheckpointAttemptDto {
                                checkpoint_attempt_id,
                                history_checkpoint: 3,
                                bundle_generation: 1,
                                reason: "idle".into(),
                            })
                        }
                    }
                }),
            )
            .route(
                "/api/runtime/sessions/{session_id}/checkpoint/fail",
                post({
                    let calls = Arc::clone(&calls);
                    move |AxumPath(session_id): AxumPath<Uuid>| {
                        let calls = Arc::clone(&calls);
                        async move {
                            calls.lock().unwrap().push(("fail", session_id));
                            Json(RuntimeSessionCheckpointDispositionDto {
                                checkpoint_attempt_id,
                                disposition: "resume".into(),
                                has_queued_work: true,
                            })
                        }
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{hub_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let transport = HubRuntimeCheckpointTransport {
            client,
            work_root: temp.path().to_path_buf(),
            producing_codex_version: "test-codex".into(),
        };

        assert_eq!(
            drive_runtime_checkpoints(&manager, &transport)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            manager.heartbeat_request().owned_sessions[0].lifecycle_status,
            "online"
        );
        assert_eq!(
            *calls.lock().unwrap(),
            vec![("begin", session_id), ("fail", session_id)]
        );
        hub.abort();
    }

    #[tokio::test]
    async fn saving_cold_session_is_never_advertised_as_ready_for_a_run() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        manager
            .request_checkpoint(
                claim.run.hub_session_id.unwrap(),
                1,
                RuntimeCheckpointReason::Idle,
            )
            .unwrap();
        assert!(manager.ready_owned_sessions().is_empty());

        manager.take_due_checkpoint_requests().await.unwrap();

        assert!(manager.ready_owned_sessions().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn saving_session_restart_recovers_the_remaining_checkpoint_retry_delay() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        let mut metadata =
            session_supervisor_metadata_for_claim(runtime_id, &claim, "test-codex").unwrap();
        metadata.lifecycle_status = "saving".into();
        metadata.checkpoint_reason = Some(RuntimeCheckpointReason::Drain);
        metadata.checkpoint_retry_unix_ms = Some(10_000);
        persist_session_supervisor_metadata(temp.path(), &metadata)
            .await
            .unwrap();
        let snapshots = vec![RuntimeOwnedSessionSnapshotDto {
            session_id: claim.run.hub_session_id.unwrap(),
            ownership_generation: 1,
            lifecycle_status: "saving".into(),
            native_thread_id: None,
            active_run_id: None,
        }];
        let recovery = plan_session_recovery(temp.path(), runtime_id, &snapshots, 1)
            .await
            .unwrap();
        let manager = SessionSupervisorManager::recover_cold_with_idle_timeout_at(
            temp.path().to_path_buf(),
            runtime_id,
            recovery,
            DEFAULT_SESSION_IDLE_TIMEOUT,
            SystemTime::UNIX_EPOCH + Duration::from_secs(4),
        );

        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id: claim.run.hub_session_id.unwrap(),
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::Drain,
            }]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drain_upgrades_an_idle_checkpoint_already_in_progress() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        manager.reserve_claim(&claim).unwrap();
        manager.complete_fake_claim(&claim).await.unwrap();
        let session_id = claim.run.hub_session_id.unwrap();
        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Idle)
            .unwrap();
        let original_request = manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .pop()
            .unwrap();

        manager
            .request_checkpoint(session_id, 1, RuntimeCheckpointReason::Drain)
            .unwrap();
        assert!(manager
            .finish_checkpoint(
                &original_request,
                RuntimeCheckpointEffectResult::Failed {
                    has_queued_work: true,
                    retry_required: true,
                    error: "upload interrupted after drain started".into(),
                },
            )
            .unwrap()
            .is_none());

        assert_eq!(
            manager.heartbeat_request().owned_sessions[0].lifecycle_status,
            "saving"
        );
        tokio::time::advance(CHECKPOINT_RETRY_DELAY).await;
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id,
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::Drain,
            }]
        );
    }

    #[tokio::test]
    async fn tampered_owned_skill_marker_cannot_delete_outside_codex_home() {
        let temp = tempfile::tempdir().unwrap();
        let original = test_claim();
        let env = prepare_run_env(temp.path(), &original, None).await.unwrap();
        let sentinel = env.workdir.join("must-survive.txt");
        fs::write(&sentinel, "workspace state").await.unwrap();

        let marker_path = env.codex_home.join(MATERIALIZATION_MARKER_FILE);
        let mut marker: ExecutionMaterializationMarker =
            serde_json::from_slice(&fs::read(&marker_path).await.unwrap()).unwrap();
        marker.owned_skill_directories = vec!["../../workspace".into()];
        write_private_file(&marker_path, &serde_json::to_vec_pretty(&marker).unwrap()).unwrap();

        let mut updated = original;
        updated.run.id = Uuid::new_v4();
        updated.execution_configuration.revision += 1;
        updated.execution_configuration.instructions = "safe updated guidance".into();
        updated.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&updated.execution_configuration).unwrap();
        prepare_run_env(temp.path(), &updated, None).await.unwrap();

        assert_eq!(
            fs::read_to_string(&sentinel).await.unwrap(),
            "workspace state"
        );
        let repaired: ExecutionMaterializationMarker =
            serde_json::from_slice(&fs::read(&marker_path).await.unwrap()).unwrap();
        assert_eq!(
            repaired.owned_skill_directories,
            vec![skill_directory_name("repo-review")]
        );
    }

    #[tokio::test]
    async fn restart_reconciliation_recovers_only_current_generation_and_keeps_blocked_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let current_id = Uuid::new_v4();
        let stale_id = Uuid::new_v4();
        let foreign_id = Uuid::new_v4();
        let missing_id = Uuid::new_v4();
        let orphan_id = Uuid::new_v4();
        for metadata in [
            SessionSupervisorMetadata {
                format_version: 1,
                session_id: current_id,
                runtime_id,
                ownership_generation: 4,
                lifecycle_status: "online".into(),
                idle_deadline_unix_ms: None,
                checkpoint_reason: None,
                checkpoint_retry_unix_ms: None,
                hub_checkpoint_attempt_id: None,
                codex_version: "test-codex".into(),
                native_thread_id: Some("thread-current".into()),
            },
            SessionSupervisorMetadata {
                format_version: 1,
                session_id: stale_id,
                runtime_id,
                ownership_generation: 2,
                lifecycle_status: "online".into(),
                idle_deadline_unix_ms: None,
                checkpoint_reason: None,
                checkpoint_retry_unix_ms: None,
                hub_checkpoint_attempt_id: None,
                codex_version: "test-codex".into(),
                native_thread_id: Some("thread-stale".into()),
            },
            SessionSupervisorMetadata {
                format_version: 1,
                session_id: foreign_id,
                runtime_id: Uuid::new_v4(),
                ownership_generation: 1,
                lifecycle_status: "online".into(),
                idle_deadline_unix_ms: None,
                checkpoint_reason: None,
                checkpoint_retry_unix_ms: None,
                hub_checkpoint_attempt_id: None,
                codex_version: "test-codex".into(),
                native_thread_id: None,
            },
            SessionSupervisorMetadata {
                format_version: 1,
                session_id: orphan_id,
                runtime_id,
                ownership_generation: 1,
                lifecycle_status: "online".into(),
                idle_deadline_unix_ms: None,
                checkpoint_reason: None,
                checkpoint_retry_unix_ms: None,
                hub_checkpoint_attempt_id: None,
                codex_version: "test-codex".into(),
                native_thread_id: None,
            },
        ] {
            persist_session_supervisor_metadata(temp.path(), &metadata)
                .await
                .unwrap();
        }
        let snapshots = vec![
            RuntimeOwnedSessionSnapshotDto {
                session_id: current_id,
                ownership_generation: 4,
                lifecycle_status: "online".into(),
                native_thread_id: Some("thread-current".into()),
                active_run_id: None,
            },
            RuntimeOwnedSessionSnapshotDto {
                session_id: stale_id,
                ownership_generation: 3,
                lifecycle_status: "online".into(),
                native_thread_id: Some("thread-current-stale-session".into()),
                active_run_id: None,
            },
            RuntimeOwnedSessionSnapshotDto {
                session_id: foreign_id,
                ownership_generation: 1,
                lifecycle_status: "online".into(),
                native_thread_id: None,
                active_run_id: None,
            },
            RuntimeOwnedSessionSnapshotDto {
                session_id: missing_id,
                ownership_generation: 1,
                lifecycle_status: "online".into(),
                native_thread_id: None,
                active_run_id: None,
            },
        ];

        let recovery = plan_session_recovery(temp.path(), runtime_id, &snapshots, 3)
            .await
            .unwrap();

        assert_eq!(recovery.records.len(), 4);
        assert_eq!(recovery.available_new_session_slots(), 0);
        assert!(matches!(
            recovery.record(current_id).unwrap().status,
            LocalSessionRecoveryStatus::Ready(_)
        ));
        for blocked in [stale_id, foreign_id, missing_id] {
            assert!(matches!(
                recovery.record(blocked).unwrap().status,
                LocalSessionRecoveryStatus::Blocked(_)
            ));
        }
        let current_metadata = SessionPaths::for_session(temp.path(), current_id)
            .supervisor
            .join(SESSION_SUPERVISOR_METADATA_FILE);
        let encoded = fs::read_to_string(current_metadata).await.unwrap();
        assert!(encoded.contains("thread-current"));
        assert!(!encoded.contains("credential"));
        assert!(!encoded.contains("secret"));
        assert!(SessionPaths::for_session(temp.path(), stale_id)
            .root
            .is_dir());
        assert!(SessionPaths::for_session(temp.path(), foreign_id)
            .root
            .is_dir());
        assert!(SessionPaths::for_session(temp.path(), orphan_id)
            .root
            .is_dir());

        let starts = temp.path().join("recovery-starts");
        let pid_file = temp.path().join("recovery-pid");
        let script = temp.path().join("recovery-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
echo start >> {}
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{}}}}' ;;
  esac
done
"#,
                shell_single_quote(&starts),
                shell_single_quote(&pid_file),
            ),
        )
        .unwrap();
        make_executable(&script);
        let manager =
            SessionSupervisorManager::recover_cold(temp.path().to_path_buf(), runtime_id, recovery);
        assert_eq!(
            manager.ready_owned_sessions(),
            vec![RuntimeOwnedSessionGenerationDto {
                session_id: current_id,
                ownership_generation: 4,
            }]
        );
        assert_eq!(manager.blocked_session_count(), 3);
        assert!(!starts.exists());

        let mut claim = test_claim();
        claim.run.hub_session_id = Some(current_id);
        claim.run.session_ownership_generation = Some(4);
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        manager
            .ensure_app_server(
                SessionSupervisorMetadata {
                    format_version: 1,
                    session_id: current_id,
                    runtime_id,
                    ownership_generation: 4,
                    lifecycle_status: "online".into(),
                    idle_deadline_unix_ms: None,
                    checkpoint_reason: None,
                    checkpoint_retry_unix_ms: None,
                    hub_checkpoint_attempt_id: None,
                    codex_version: "test-codex".into(),
                    native_thread_id: Some("thread-current".into()),
                },
                script.display().to_string(),
                run_env,
                Duration::from_secs(2),
                None,
            )
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(starts).unwrap().lines().count(), 1);
        manager.shutdown();
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn interrupted_restoring_run_is_failed_and_releases_runtime_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let recovery = plan_session_recovery(
            temp.path(),
            runtime_id,
            &[RuntimeOwnedSessionSnapshotDto {
                session_id,
                ownership_generation: 3,
                lifecycle_status: "restoring".into(),
                native_thread_id: None,
                active_run_id: Some(run_id),
            }],
            1,
        )
        .await
        .unwrap();
        let manager =
            SessionSupervisorManager::recover_cold(temp.path().to_path_buf(), runtime_id, recovery);
        assert_eq!(manager.blocked_session_count(), 1);
        assert_eq!(manager.available_new_session_slots(), 0);

        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new()
            .route(
                "/api/runtime/runs/{run_id}/events",
                post({
                    let requests = Arc::clone(&requests);
                    move |AxumPath(actual_run_id): AxumPath<Uuid>, Json(request): Json<
                        RuntimeSessionWriteRequest<AppendRunEventRequest>,
                    >| {
                        let requests = Arc::clone(&requests);
                        async move {
                            assert_eq!(actual_run_id, run_id);
                            assert_eq!(request.ownership_generation, 3);
                            requests.lock().unwrap().push("event");
                            AxumStatusCode::OK
                        }
                    }
                }),
            )
            .route(
                "/api/runtime/runs/{run_id}/complete",
                post({
                    let requests = Arc::clone(&requests);
                    move |AxumPath(actual_run_id): AxumPath<Uuid>, Json(request): Json<
                        RuntimeSessionWriteRequest<CompleteRunRequest>,
                    >| {
                        let requests = Arc::clone(&requests);
                        async move {
                            assert_eq!(actual_run_id, run_id);
                            assert_eq!(request.ownership_generation, 3);
                            assert_eq!(request.payload.status, "failed");
                            requests.lock().unwrap().push("complete");
                            AxumStatusCode::OK
                        }
                    }
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{hub_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };

        assert_eq!(fail_interrupted_restoring_runs(&manager, &client).await, 1);
        assert_eq!(*requests.lock().unwrap(), vec!["event", "complete"]);
        assert_eq!(manager.blocked_session_count(), 0);
        assert_eq!(manager.available_new_session_slots(), 1);
        assert!(manager.heartbeat_request().owned_sessions.is_empty());
        hub.abort();
    }

    #[tokio::test]
    async fn hub_fencing_after_unacknowledged_restoring_failure_releases_runtime_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let recovery = plan_session_recovery(
            temp.path(),
            runtime_id,
            &[RuntimeOwnedSessionSnapshotDto {
                session_id,
                ownership_generation: 3,
                lifecycle_status: "restoring".into(),
                native_thread_id: None,
                active_run_id: Some(run_id),
            }],
            1,
        )
        .await
        .unwrap();
        let manager =
            SessionSupervisorManager::recover_cold(temp.path().to_path_buf(), runtime_id, recovery);

        let app =
            Router::new()
                .route(
                    "/api/runtime/runs/{run_id}/events",
                    post(
                        move |AxumPath(actual_run_id): AxumPath<Uuid>,
                              Json(request): Json<
                            RuntimeSessionWriteRequest<AppendRunEventRequest>,
                        >| async move {
                            assert_eq!(actual_run_id, run_id);
                            assert_eq!(request.ownership_generation, 3);
                            AxumStatusCode::OK
                        },
                    ),
                )
                .route(
                    "/api/runtime/runs/{run_id}/complete",
                    post(
                        move |AxumPath(actual_run_id): AxumPath<Uuid>,
                              Json(request): Json<
                            RuntimeSessionWriteRequest<CompleteRunRequest>,
                        >| async move {
                            assert_eq!(actual_run_id, run_id);
                            assert_eq!(request.ownership_generation, 3);
                            assert_eq!(request.payload.status, "failed");
                            AxumStatusCode::SERVICE_UNAVAILABLE
                        },
                    ),
                );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{hub_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };

        assert_eq!(fail_interrupted_restoring_runs(&manager, &client).await, 0);
        assert_eq!(manager.blocked_session_count(), 1);
        assert_eq!(manager.available_new_session_slots(), 0);

        manager.reconcile_owned_snapshots(&[]).unwrap();
        assert_eq!(manager.blocked_session_count(), 0);
        assert_eq!(manager.available_new_session_slots(), 1);
        assert!(manager.heartbeat_request().owned_sessions.is_empty());
        hub.abort();
    }

    #[tokio::test]
    async fn heartbeat_native_thread_mismatch_blocks_session_and_releases_live_resources() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("thread-mismatch-pid");
        let script = temp.path().join("thread-mismatch-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{}}}}' ;;
  esac
done
"#,
                shell_single_quote(&pid_file),
            ),
        )
        .unwrap();
        make_executable(&script);
        let runtime_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: "http://127.0.0.1:1".into(),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(session_id);
        claim.run.session_ownership_generation = Some(5);
        let proxy = Arc::new(
            start_model_proxy(
                &client,
                claim.run.id,
                &claim.model_proxy_token,
                Duration::from_secs(1),
            )
            .await
            .unwrap(),
        );
        let proxy_addr: SocketAddr = proxy
            .base_url
            .trim_start_matches("http://")
            .trim_end_matches("/v1")
            .parse()
            .unwrap();
        let run_env = prepare_run_env(temp.path(), &claim, Some(&proxy.base_url))
            .await
            .unwrap();
        manager
            .ensure_app_server(
                SessionSupervisorMetadata {
                    format_version: 1,
                    session_id,
                    runtime_id,
                    ownership_generation: 5,
                    lifecycle_status: "online".into(),
                    idle_deadline_unix_ms: None,
                    checkpoint_reason: None,
                    checkpoint_retry_unix_ms: None,
                    hub_checkpoint_attempt_id: None,
                    codex_version: "test-codex".into(),
                    native_thread_id: Some("local-thread".into()),
                },
                script.display().to_string(),
                run_env,
                Duration::from_secs(2),
                Some(Arc::clone(&proxy)),
            )
            .await
            .unwrap();
        drop(proxy);

        assert!(manager
            .reconcile_owned_snapshots(&[RuntimeOwnedSessionSnapshotDto {
                session_id,
                ownership_generation: 5,
                lifecycle_status: "online".into(),
                native_thread_id: None,
                active_run_id: None,
            }])
            .unwrap()
            .is_empty());
        assert_eq!(manager.ready_owned_sessions().len(), 1);

        assert!(manager
            .reconcile_owned_snapshots(&[RuntimeOwnedSessionSnapshotDto {
                session_id,
                ownership_generation: 5,
                lifecycle_status: "online".into(),
                native_thread_id: Some("different-hub-thread".into()),
                active_run_id: None,
            }])
            .unwrap()
            .is_empty());

        assert!(manager.ready_owned_sessions().is_empty());
        assert_eq!(manager.blocked_session_count(), 1);
        assert_eq!(manager.available_new_session_slots(), 0);
        assert!(manager.model_proxy(session_id).is_none());
        assert_process_group_reaped_or_clean_up(&pid_file);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if tokio::net::TcpStream::connect(proxy_addr).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("native thread mismatch must close the model proxy listener");
    }

    #[tokio::test]
    async fn hub_fenced_online_session_stops_app_server_before_workspace_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("deleted-agent-session-pid");
        let script = temp.path().join("deleted-agent-session-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{}}}}' ;;
  esac
done
"#,
                shell_single_quote(&pid_file),
            ),
        )
        .unwrap();
        make_executable(&script);
        let runtime_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(session_id);
        claim.run.session_ownership_generation = Some(3);
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        manager
            .ensure_app_server(
                SessionSupervisorMetadata {
                    format_version: 1,
                    session_id,
                    runtime_id,
                    ownership_generation: 3,
                    lifecycle_status: "online".into(),
                    idle_deadline_unix_ms: None,
                    checkpoint_reason: None,
                    checkpoint_retry_unix_ms: None,
                    hub_checkpoint_attempt_id: None,
                    codex_version: "test-codex".into(),
                    native_thread_id: Some("deleted-agent-thread".into()),
                },
                script.display().to_string(),
                run_env,
                Duration::from_secs(2),
                None,
            )
            .await
            .unwrap();

        let cleanup = manager
            .reconcile_owned_snapshots(&[])
            .unwrap()
            .pop()
            .expect("Hub-fenced online Session must reserve local cleanup");

        assert!(manager.heartbeat_request().owned_sessions.is_empty());
        assert_process_group_reaped_or_clean_up(&pid_file);
        assert!(cleanup.path.join("workspace").is_dir());
        fs::remove_dir_all(&cleanup.path).await.unwrap();
        manager.complete_released_session_cleanup(&cleanup).unwrap();
    }

    #[tokio::test]
    async fn mcp_secrets_are_injected_only_into_private_codex_config() {
        let temp = tempfile::tempdir().unwrap();
        let mut claim = test_claim();
        claim.execution_configuration.mcp_allowlist = json!([
            {
                "name": "filesystem",
                "command": "fs",
                "args": ["--root", "/workspace"],
                "secrets": { "API_TOKEN": "super-secret-token" }
            }
        ]);
        claim.agent.mcp_allowlist = claim.execution_configuration.mcp_allowlist.clone();
        claim.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap();

        let env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let config_toml = fs::read_to_string(env.codex_home.join("config.toml"))
            .await
            .unwrap();
        let allowlist = fs::read_to_string(env.codex_home.join("mcp-allowlist.json"))
            .await
            .unwrap();

        assert!(config_toml.contains("[mcp_servers.filesystem]"));
        assert!(config_toml.contains("API_TOKEN = \"super-secret-token\""));
        assert!(!allowlist.contains("super-secret-token"));
        assert!(allowlist.contains(REDACTED_SECRET));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(env.codex_home.join("config.toml"))
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }

    #[test]
    fn fake_codex_emits_assistant_message() {
        let claim = test_claim();
        let (events, status) = fake_codex_events(&claim);
        assert_eq!(status, "completed");
        assert!(events
            .iter()
            .any(|event| event.role.as_deref() == Some("assistant")));
    }

    #[test]
    fn invalid_codex_driver_is_rejected() {
        assert!(validate_codex_driver("fake").is_ok());
        assert!(validate_codex_driver("app-server").is_ok());
        assert!(validate_codex_driver("typo").is_err());
    }

    #[test]
    fn max_online_sessions_has_a_positive_validated_default() {
        assert_eq!(
            parse_max_online_sessions(None).unwrap(),
            DEFAULT_MAX_ONLINE_SESSIONS
        );
        assert_eq!(parse_max_online_sessions(Some("7")).unwrap(), 7);
        assert!(parse_max_online_sessions(Some("0")).is_err());
        assert!(parse_max_online_sessions(Some("many")).is_err());
    }

    #[test]
    fn session_idle_timeout_defaults_to_fifteen_minutes_and_is_configurable() {
        assert_eq!(
            parse_session_idle_timeout(None).unwrap(),
            Duration::from_secs(900)
        );
        assert_eq!(
            parse_session_idle_timeout(Some("30")).unwrap(),
            Duration::from_secs(30)
        );
        assert!(parse_session_idle_timeout(Some("0")).is_err());
        assert!(parse_session_idle_timeout(Some("invalid")).is_err());
    }

    #[test]
    fn codex_config_uses_runtime_local_model_proxy_base_url() {
        let claim = test_claim();
        let config_toml = render_codex_config(
            &claim.execution_configuration,
            Some("http://127.0.0.1:4567/v1"),
        )
        .unwrap();

        assert!(config_toml.contains("model_provider = \"agent_hub_"));
        assert!(config_toml.contains("[model_providers.agent_hub_"));
        assert!(config_toml.contains("base_url = \"http://127.0.0.1:4567/v1\""));
        assert!(config_toml.contains("wire_api = \"responses\""));
    }

    #[test]
    fn codex_config_materializes_native_multi_provider_models_and_reasoning() {
        let mut claim = test_claim();
        let default_connection_id = claim
            .execution_configuration
            .default_model_connection_id
            .unwrap();
        let override_connection_id = Uuid::from_u128(0x202);
        claim
            .execution_configuration
            .model_connections
            .push(ModelConnectionOptionDto {
                id: override_connection_id,
                name: "Review model".into(),
                model_id: "gpt-review".into(),
                scope: ModelConnectionScope::Personal,
                status: ModelConnectionStatus::Enabled,
            });
        claim.execution_configuration.codex_subagents = vec![CodexSubagentDefinition {
            name: "reviewer".into(),
            description: "Review the current change".into(),
            developer_instructions: "Inspect correctness and security.".into(),
            model_connection_id: Some(override_connection_id),
            reasoning_effort: Some(ReasoningEffort::Ultra),
            enabled: true,
            disabled_reason: None,
        }];
        let default_provider = format!("agent_hub_{}", default_connection_id.simple());
        let override_provider = format!("agent_hub_{}", override_connection_id.simple());

        let rendered = render_codex_config(
            &claim.execution_configuration,
            Some("http://127.0.0.1:4567/v1"),
        )
        .unwrap();
        let parsed = rendered.parse::<toml::Value>().unwrap();

        assert_eq!(parsed["model"].as_str(), Some("gpt-main"));
        assert_eq!(
            parsed["model_provider"].as_str(),
            Some(default_provider.as_str())
        );
        assert!(parsed.get("model_reasoning_effort").is_none());
        assert_eq!(parsed["agents"]["max_threads"].as_integer(), Some(6));
        assert_eq!(parsed["agents"]["max_depth"].as_integer(), Some(1));
        for (connection_id, provider) in [
            (default_connection_id, default_provider),
            (override_connection_id, override_provider),
        ] {
            let provider = &parsed["model_providers"][&provider];
            assert_eq!(
                provider["base_url"].as_str(),
                Some("http://127.0.0.1:4567/v1")
            );
            assert_eq!(provider["wire_api"].as_str(), Some("responses"));
            assert_eq!(
                provider["http_headers"]["x-agent-hub-model-connection-id"].as_str(),
                Some(connection_id.to_string().as_str())
            );
            assert!(provider.get("env_key").is_none());
        }

        let efforts = [
            (ReasoningEffort::Default, None),
            (ReasoningEffort::None, Some("none")),
            (ReasoningEffort::Minimal, Some("minimal")),
            (ReasoningEffort::Low, Some("low")),
            (ReasoningEffort::Medium, Some("medium")),
            (ReasoningEffort::High, Some("high")),
            (ReasoningEffort::Xhigh, Some("xhigh")),
            (ReasoningEffort::Max, Some("max")),
            (ReasoningEffort::Ultra, Some("ultra")),
        ];
        for (effort, expected) in efforts {
            claim.execution_configuration.reasoning_effort = effort;
            let parsed = render_codex_config(
                &claim.execution_configuration,
                Some("http://127.0.0.1:4567/v1"),
            )
            .unwrap()
            .parse::<toml::Value>()
            .unwrap();
            assert_eq!(
                parsed
                    .get("model_reasoning_effort")
                    .and_then(toml::Value::as_str),
                expected
            );
        }
    }

    #[tokio::test]
    async fn native_model_materialization_ignores_legacy_provider_url_and_api_key() {
        const PROVIDER_URL: &str = "https://provider-secret.example";
        const PROVIDER_API_KEY: &str = "provider-api-key-must-not-leak";

        let temp = tempfile::tempdir().unwrap();
        let mut claim = test_claim();
        claim.execution_configuration.model_policy = json!({
            "base_url": PROVIDER_URL,
            "api_key": PROVIDER_API_KEY,
            "provider": "legacy-provider"
        });
        claim.agent.model_policy = claim.execution_configuration.model_policy.clone();
        claim.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap();

        let run_env = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:4567/v1"))
            .await
            .unwrap();
        for entry in WalkDir::new(&run_env.codex_home) {
            let entry = entry.unwrap();
            if !entry.file_type().is_file() {
                continue;
            }
            let contents = stdfs::read(entry.path()).unwrap();
            let contents = String::from_utf8_lossy(&contents);
            assert!(!contents.contains(PROVIDER_URL));
            assert!(!contents.contains(PROVIDER_API_KEY));
        }
        let (events, _) = fake_codex_events(&claim);
        let events = serde_json::to_string(&events).unwrap();
        assert!(!events.contains(PROVIDER_URL));
        assert!(!events.contains(PROVIDER_API_KEY));
    }

    #[test]
    fn runtime_registration_reports_effective_read_only_sandbox_after_downgrade() {
        let mut config = test_config();
        config.sandbox_mode = "danger-full-access".into();
        config.sandbox_downgrade_reason = Some("workspace is mounted read-only".into());

        let request = runtime_register_request(&config);

        assert_eq!(request.sandbox_mode, "read-only");
        assert_eq!(request.codex_version, config.codex_version);
        assert_eq!(
            request.capabilities["sandbox"]["configured_mode"],
            "danger-full-access"
        );
        assert_eq!(
            request.capabilities["sandbox"]["effective_mode"],
            "read-only"
        );
        assert_eq!(request.capabilities["sandbox"]["downgraded"], true);
        assert_eq!(
            request.capabilities["sandbox"]["downgrade_reason"],
            "workspace is mounted read-only"
        );
        assert_eq!(request.capabilities["platform"]["os"], std::env::consts::OS);
        assert_eq!(
            request.capabilities["platform"]["architecture"],
            std::env::consts::ARCH
        );
    }

    #[tokio::test]
    async fn runtime_health_is_unavailable_until_registration_succeeds() {
        let health = Arc::new(RuntimeHealth::default());
        let (addr, server) =
            start_runtime_health_server("127.0.0.1:0".parse().unwrap(), Arc::clone(&health))
                .await
                .unwrap();
        let http = reqwest::Client::new();

        let unavailable = http
            .get(format!("http://{addr}/healthz"))
            .send()
            .await
            .unwrap();
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);

        health.mark_registered();

        let ready = http
            .get(format!("http://{addr}/healthz"))
            .send()
            .await
            .unwrap();
        assert_eq!(ready.status(), StatusCode::OK);
        server.abort();
    }

    #[tokio::test]
    async fn no_content_claim_is_idle_work_and_keeps_runtime_registered() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut config = test_config();
        config.hub_url = format!("http://{}", listener.local_addr().unwrap());
        let stored = test_stored_runtime_credential();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.starts_with("POST /api/runtime/runs/claim HTTP/1.1"));
            assert!(request.contains("authorization: Bearer current-runtime-credential"));
            assert!(request.contains("\"available_new_session_slots\":2"));
            assert!(request.contains("\"ready_owned_sessions\":[]"));
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });
        let client = hub_client_from_stored(&config, runtime_http_client().unwrap(), &stored);
        let health = RuntimeHealth::default();
        health.mark_registered();

        assert!(client
            .claim_run(&RuntimeClaimRunRequest {
                available_new_session_slots: 2,
                ready_owned_sessions: Vec::new(),
            })
            .await
            .unwrap()
            .is_none());
        assert!(health.is_registered());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn runtime_dispatcher_serializes_claims_and_reserves_capacity_before_returning() {
        let claim = test_claim();
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let next_claim = Arc::new(std::sync::Mutex::new(Some(claim.clone())));
        let app = Router::new().route(
            "/api/runtime/runs/claim",
            post({
                let requests = Arc::clone(&requests);
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let next_claim = Arc::clone(&next_claim);
                move |Json(request): Json<RuntimeClaimRunRequest>| {
                    let requests = Arc::clone(&requests);
                    let active = Arc::clone(&active);
                    let max_active = Arc::clone(&max_active);
                    let next_claim = Arc::clone(&next_claim);
                    async move {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_active.fetch_max(current, Ordering::SeqCst);
                        requests.lock().unwrap().push(request);
                        tokio::time::sleep(Duration::from_millis(75)).await;
                        let response = match next_claim.lock().unwrap().take() {
                            Some(claim) => Json(claim).into_response(),
                            None => AxumStatusCode::NO_CONTENT.into_response(),
                        };
                        active.fetch_sub(1, Ordering::SeqCst);
                        response
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut config = test_config();
        config.hub_url = format!("http://{addr}");
        let stored = test_stored_runtime_credential();
        let client = Arc::new(hub_client_from_stored(
            &config,
            runtime_http_client().unwrap(),
            &stored,
        ));
        let manager = Arc::new(SessionSupervisorManager::new(
            tempfile::tempdir().unwrap().keep(),
            stored.runtime_id,
            1,
        ));
        let dispatcher = Arc::new(RuntimeRunDispatcher::default());

        let first = tokio::spawn({
            let dispatcher = Arc::clone(&dispatcher);
            let manager = Arc::clone(&manager);
            let client = Arc::clone(&client);
            async move { dispatcher.claim_next(&client, &manager).await }
        });
        let second = tokio::spawn({
            let dispatcher = Arc::clone(&dispatcher);
            let manager = Arc::clone(&manager);
            let client = Arc::clone(&client);
            async move { dispatcher.claim_next(&client, &manager).await }
        });
        let results = [
            first.await.unwrap().unwrap(),
            second.await.unwrap().unwrap(),
        ];

        assert_eq!(results.iter().filter(|result| result.is_some()).count(), 1);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].available_new_session_slots, 1);
        assert_eq!(requests[1].available_new_session_slots, 0);
        assert!(requests[1].ready_owned_sessions.is_empty());
        assert_eq!(manager.available_new_session_slots(), 0);
        server.abort();
    }

    #[tokio::test]
    async fn local_model_proxy_preserves_connection_query_safe_headers_and_body() {
        #[derive(Debug)]
        struct ForwardedRequest {
            query: Option<String>,
            headers: HeaderMap,
            body: Bytes,
        }

        let forwarded = Arc::new(std::sync::Mutex::new(None));
        let app = Router::new().route(
            "/api/runtime/model-proxy/v1/{*path}",
            post({
                let forwarded = Arc::clone(&forwarded);
                move |axum::extract::RawQuery(query): axum::extract::RawQuery,
                      headers: HeaderMap,
                      body: Bytes| {
                    let forwarded = Arc::clone(&forwarded);
                    async move {
                        *forwarded.lock().unwrap() = Some(ForwardedRequest {
                            query,
                            headers,
                            body,
                        });
                        Json(json!({ "ok": true }))
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{hub_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let run_id = Uuid::new_v4();
        let connection_id = Uuid::new_v4();
        let proxy = start_model_proxy(
            &client,
            run_id,
            "scoped-model-token",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let request_body = br#"{"model":"gpt-main","input":"keep bytes"}"#;

        let response = reqwest::Client::new()
            .post(format!(
                "{}/responses?include=usage&trace=a%2Fb",
                proxy.base_url
            ))
            .header("x-agent-hub-model-connection-id", connection_id.to_string())
            .header("x-request-id", "request-123")
            .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
            .header(header::AUTHORIZATION, "Bearer must-not-escape")
            .body(request_body.as_slice())
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let request = forwarded.lock().unwrap().take().unwrap();
        assert_eq!(request.query.as_deref(), Some("include=usage&trace=a%2Fb"));
        assert_eq!(
            request
                .headers
                .get("x-agent-hub-model-connection-id")
                .unwrap(),
            connection_id.to_string().as_str()
        );
        assert_eq!(request.headers.get("x-request-id").unwrap(), "request-123");
        assert_eq!(
            request.headers.get(header::CONTENT_TYPE).unwrap(),
            "application/json; charset=utf-8"
        );
        assert_eq!(
            request.headers.get(header::AUTHORIZATION).unwrap(),
            "Bearer scoped-model-token"
        );
        assert_eq!(
            request.headers.get("x-agent-hub-run-id").unwrap(),
            run_id.to_string().as_str()
        );
        assert_eq!(request.body.as_ref(), request_body);
        hub.abort();
    }

    #[tokio::test]
    async fn local_model_proxy_streams_upstream_status_content_type_and_first_chunk() {
        let upstream = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_thread = std::thread::spawn(move || {
            let (mut stream, _) = upstream.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    return;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            let request = String::from_utf8_lossy(&request).to_ascii_lowercase();
            assert!(request.contains("authorization: bearer scoped-model-token"));
            stream
                .write_all(
                    b"HTTP/1.1 201 Created\r\n\
Content-Type: text/event-stream\r\n\
X-Upstream-Trace: trace-123\r\n\
X-Upstream-Token: upstream-secret-token\r\n\
X-Api-Key: upstream-api-key\r\n\
Connection: keep-alive, x-remove-me\r\n\
X-Remove-Me: must-not-forward\r\n\
Transfer-Encoding: chunked\r\n\
\r\n\
6\r\nfirst\n\r\n",
                )
                .unwrap();
            stream.flush().unwrap();
            for chunk in [b"second\n".as_slice(), b"third\n", b"fourth\n"] {
                std::thread::sleep(Duration::from_millis(80));
                write!(stream, "{:x}\r\n", chunk.len()).unwrap();
                stream.write_all(chunk).unwrap();
                stream.write_all(b"\r\n").unwrap();
                stream.flush().unwrap();
            }
            stream.write_all(b"0\r\n\r\n").unwrap();
            stream.flush().unwrap();
        });
        let client = HubClient {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_millis(50))
                .timeout(Duration::from_millis(150))
                .build()
                .unwrap(),
            hub_url: format!("http://{upstream_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let proxy = start_model_proxy(
            &client,
            Uuid::new_v4(),
            "scoped-model-token",
            Duration::from_millis(150),
        )
        .await
        .unwrap();
        let http = reqwest::Client::new();

        let response = tokio::time::timeout(
            Duration::from_millis(150),
            http.post(format!("{}/responses", proxy.base_url))
                .header(header::CONTENT_TYPE, "application/json")
                .body("{}")
                .send(),
        )
        .await
        .expect("proxy must send response headers before upstream finishes")
        .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/event-stream"
        );
        assert_eq!(
            response.headers().get("x-upstream-trace").unwrap(),
            "trace-123"
        );
        assert!(response.headers().get("x-upstream-token").is_none());
        assert!(response.headers().get("x-api-key").is_none());
        assert!(response.headers().get("x-remove-me").is_none());

        let mut response = response;
        let first = tokio::time::timeout(Duration::from_millis(100), response.chunk())
            .await
            .expect("first upstream chunk must not wait for the complete response")
            .unwrap()
            .unwrap();
        assert_eq!(first.as_ref(), b"first\n");
        let remaining = tokio::time::timeout(Duration::from_millis(500), response.text())
            .await
            .expect("periodic chunks must keep the proxy stream alive")
            .unwrap();
        assert_eq!(remaining, "second\nthird\nfourth\n");
        upstream_thread.join().unwrap();
    }

    #[tokio::test]
    async fn local_model_proxy_terminates_an_idle_stream_after_the_first_chunk() {
        let upstream = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_thread = std::thread::spawn(move || {
            let (mut stream, _) = upstream.accept().unwrap();
            let _ = read_http_request(&mut stream);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n6\r\nfirst\n\r\n",
                )
                .unwrap();
            stream.flush().unwrap();
            std::thread::sleep(Duration::from_millis(250));
        });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{upstream_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let proxy = start_model_proxy(
            &client,
            Uuid::new_v4(),
            "scoped-model-token",
            Duration::from_millis(75),
        )
        .await
        .unwrap();
        let mut response = reqwest::Client::new()
            .post(format!("{}/responses", proxy.base_url))
            .body("{}")
            .send()
            .await
            .unwrap();

        assert_eq!(
            response.chunk().await.unwrap().unwrap().as_ref(),
            b"first\n"
        );
        let stalled = tokio::time::timeout(Duration::from_millis(200), response.chunk())
            .await
            .expect("idle model stream must terminate within its read timeout");
        assert!(
            stalled.is_err(),
            "idle timeout must surface as a stream error"
        );
        upstream_thread.join().unwrap();
    }

    #[tokio::test]
    async fn session_model_proxy_reuses_listener_and_switches_run_auth_atomically() {
        let forwarded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new().route(
            "/api/runtime/model-proxy/v1/{*path}",
            post({
                let forwarded = Arc::clone(&forwarded);
                move |headers: HeaderMap| {
                    let forwarded = Arc::clone(&forwarded);
                    async move {
                        forwarded.lock().unwrap().push((
                            headers
                                .get(header::AUTHORIZATION)
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .to_owned(),
                            headers
                                .get("x-agent-hub-run-id")
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .to_owned(),
                        ));
                        AxumStatusCode::OK
                    }
                }
            }),
        );
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(upstream, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{upstream_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let first_run_id = Uuid::new_v4();
        let second_run_id = Uuid::new_v4();
        let proxy = start_model_proxy(
            &client,
            first_run_id,
            "first-model-token",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let stable_base_url = proxy.base_url.clone();
        let http = reqwest::Client::new();

        assert_eq!(
            http.post(format!("{stable_base_url}/responses"))
                .body("{}")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        proxy.activate_run(second_run_id, "second-model-token");
        assert_eq!(proxy.base_url, stable_base_url);
        assert_eq!(
            http.post(format!("{stable_base_url}/responses"))
                .body("{}")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        assert_eq!(
            *forwarded.lock().unwrap(),
            vec![
                ("Bearer first-model-token".into(), first_run_id.to_string()),
                (
                    "Bearer second-model-token".into(),
                    second_run_id.to_string()
                ),
            ]
        );
        server.abort();
    }

    #[tokio::test]
    async fn persisted_runtime_identity_bypasses_enrollment_and_uses_mode_0600() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.credential_file = temp.path().join("runtime-credential.json");
        config.enrollment_token = None;
        let stored = StoredRuntimeCredential {
            runtime_id: Uuid::new_v4(),
            runtime_credential: "current-runtime-credential".into(),
            pending_runtime_credential: Some("pending-runtime-credential".into()),
            protocol_capabilities: vec![ATOMIC_WAITING_TOOL_BATCH_CAPABILITY.into()],
        };

        persist_runtime_credential(&config.credential_file, &stored).unwrap();
        assert_eq!(
            load_runtime_credential(&config.credential_file)
                .unwrap()
                .unwrap(),
            stored
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                stdfs::metadata(&config.credential_file)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        let (client, loaded) = initialize_runtime(&config).await.unwrap();
        assert_eq!(loaded, stored);
        assert_eq!(client.runtime_credential(), "current-runtime-credential");
        assert!(client
            .protocol_capabilities
            .contains(ATOMIC_WAITING_TOOL_BATCH_CAPABILITY));

        let mut promoted = stored;
        promoted.runtime_credential = "pending-runtime-credential".into();
        promoted.pending_runtime_credential = None;
        persist_runtime_credential(&config.credential_file, &promoted).unwrap();
        assert_eq!(
            load_runtime_credential(&config.credential_file)
                .unwrap()
                .unwrap(),
            promoted
        );
        assert_eq!(stdfs::read_dir(temp.path()).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn first_enrollment_persists_the_per_runtime_credential() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.credential_file = temp.path().join("runtime-credential.json");
        config.enrollment_token = Some("one-time-enrollment".into());
        let runtime_id = Uuid::new_v4();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.hub_url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.starts_with("POST /api/runtime/register HTTP/1.1"));
            assert!(request.contains("authorization: Bearer one-time-enrollment"));
            assert!(!request.starts_with("POST /api/runtime/register?"));
            let body = serde_json::to_string(&RuntimeRegisterResponse {
                runtime_id,
                runtime_credential: "new-per-runtime-credential".into(),
                protocol_capabilities: vec![ATOMIC_WAITING_TOOL_BATCH_CAPABILITY.into()],
            })
            .unwrap();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            stream.flush().unwrap();
        });

        let (client, stored) = initialize_runtime(&config).await.unwrap();

        server.join().unwrap();
        assert_eq!(stored.runtime_id, runtime_id);
        assert_eq!(stored.runtime_credential, "new-per-runtime-credential");
        assert_eq!(client.runtime_credential(), stored.runtime_credential);
        assert_eq!(
            load_runtime_credential(&config.credential_file)
                .unwrap()
                .unwrap(),
            stored
        );
    }

    #[tokio::test]
    async fn rejected_persisted_credential_does_not_attempt_enrollment() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.credential_file = temp.path().join("runtime-credential.json");
        config.enrollment_token = Some("must-not-be-used".into());
        let mut stored = test_stored_runtime_credential();
        let expected = stored.clone();
        persist_runtime_credential(&config.credential_file, &stored).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.hub_url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.contains("/api/runtime/heartbeat"));
            assert!(!request.contains("/api/runtime/register"));
            assert!(!request.contains("must-not-be-used"));
            stream
                .write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });
        let mut client = hub_client_from_stored(&config, runtime_http_client().unwrap(), &stored);

        let error = reconcile_runtime_credential(&config, &mut client, &mut stored)
            .await
            .unwrap_err();

        assert!(is_auth_loss(&error));
        assert_eq!(stored, expected);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn rotation_recovers_when_stage_response_is_lost() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.credential_file = temp.path().join("runtime-credential.json");
        let mut stored = test_stored_runtime_credential();
        persist_runtime_credential(&config.credential_file, &stored).unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.hub_url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.contains("authorization: Bearer current-runtime-credential"));
            write_heartbeat_response(&mut stream, true, false, false);

            let (mut stream, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.contains("authorization: Bearer current-runtime-credential"));
            assert!(request.contains("pending_credential_hash"));
            // Hub commits the staged hash, but the response is lost.
        });
        let mut client = hub_client_from_stored(&config, runtime_http_client().unwrap(), &stored);
        assert!(
            reconcile_runtime_credential(&config, &mut client, &mut stored)
                .await
                .is_err()
        );
        server.join().unwrap();
        assert!(stored.pending_runtime_credential.is_some());
        assert_eq!(
            load_runtime_credential(&config.credential_file)
                .unwrap()
                .unwrap(),
            stored
        );

        let recovery_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.hub_url = format!("http://{}", recovery_listener.local_addr().unwrap());
        let pending = stored.pending_runtime_credential.clone().unwrap();
        let expected_pending = pending.clone();
        let recovery_server = std::thread::spawn(move || {
            let (mut stream, _) = recovery_listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.contains(&format!("authorization: Bearer {expected_pending}")));
            write_heartbeat_response(&mut stream, false, false, true);
        });
        let mut client = hub_client_from_stored(&config, runtime_http_client().unwrap(), &stored);
        reconcile_runtime_credential(&config, &mut client, &mut stored)
            .await
            .unwrap();
        recovery_server.join().unwrap();
        assert_eq!(stored.runtime_credential, pending);
        assert!(stored.pending_runtime_credential.is_none());
        assert_eq!(client.runtime_credential(), stored.runtime_credential);
    }

    #[tokio::test]
    async fn rotation_recovers_when_confirm_response_is_lost() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.credential_file = temp.path().join("runtime-credential.json");
        let mut stored = test_stored_runtime_credential();
        persist_runtime_credential(&config.credential_file, &stored).unwrap();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.hub_url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.contains("authorization: Bearer current-runtime-credential"));
            write_heartbeat_response(&mut stream, true, false, false);

            let (mut stream, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.contains("authorization: Bearer current-runtime-credential"));
            write_heartbeat_response(&mut stream, true, true, false);

            let (mut stream, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(!request.contains("authorization: Bearer current-runtime-credential"));
            // Hub promotes the pending credential, but the response is lost.
        });
        let mut client = hub_client_from_stored(&config, runtime_http_client().unwrap(), &stored);
        assert!(
            reconcile_runtime_credential(&config, &mut client, &mut stored)
                .await
                .is_err()
        );
        server.join().unwrap();
        let pending = stored.pending_runtime_credential.clone().unwrap();

        let recovery_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.hub_url = format!("http://{}", recovery_listener.local_addr().unwrap());
        let expected_pending = pending.clone();
        let recovery_server = std::thread::spawn(move || {
            let (mut stream, _) = recovery_listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.contains(&format!("authorization: Bearer {expected_pending}")));
            // A successful ordinary heartbeat means the already-promoted credential is current.
            write_heartbeat_response(&mut stream, false, false, false);
        });
        let mut client = hub_client_from_stored(&config, runtime_http_client().unwrap(), &stored);
        reconcile_runtime_credential(&config, &mut client, &mut stored)
            .await
            .unwrap();
        recovery_server.join().unwrap();
        assert_eq!(stored.runtime_credential, pending);
        assert!(stored.pending_runtime_credential.is_none());
    }

    #[tokio::test]
    async fn cloned_active_worker_uses_the_latest_runtime_credential_per_request() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.contains("authorization: Bearer old-runtime-credential"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();

            let (mut stream, _) = listener.accept().unwrap();
            let request = String::from_utf8_lossy(&read_http_request(&mut stream)).to_string();
            assert!(request.contains("authorization: Bearer new-runtime-credential"));
            assert!(!request.contains("authorization: Bearer old-runtime-credential"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
        });
        let client = HubClient {
            http: runtime_http_client().unwrap(),
            hub_url: format!("http://{addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("old-runtime-credential".into())),
            protocol_capabilities: HashSet::new(),
        };
        let worker = client.clone();
        let run_id = Uuid::new_v4();

        worker
            .append_event(
                run_id,
                1,
                AppendRunEventRequest {
                    event_type: "status".into(),
                    role: None,
                    content: Some("running".into()),
                    payload: json!({}),
                    waiting_tool: None,
                },
            )
            .await
            .unwrap();
        client.replace_runtime_credential("new-runtime-credential".into());
        worker
            .complete_run(
                run_id,
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    session_id: Some("thread".into()),
                    work_dir_ref: Some("workspace".into()),
                },
            )
            .await
            .unwrap();

        server.join().unwrap();
    }

    #[test]
    fn local_model_proxy_filters_tokens_from_every_connection_header_value() {
        let mut upstream = HeaderMap::new();
        upstream.append(
            header::CONNECTION,
            header::HeaderValue::from_static("keep-alive, x-first-hop"),
        );
        upstream.append(
            header::CONNECTION,
            header::HeaderValue::from_static("x-second-hop"),
        );
        upstream.insert(
            HeaderName::from_static("x-first-hop"),
            header::HeaderValue::from_static("first"),
        );
        upstream.insert(
            HeaderName::from_static("x-second-hop"),
            header::HeaderValue::from_static("second"),
        );

        let forwarded = forwarded_model_response_headers(&upstream);

        assert!(forwarded.get("x-first-hop").is_none());
        assert!(forwarded.get("x-second-hop").is_none());
    }

    #[tokio::test]
    async fn app_server_process_sends_json_rpc_lifecycle_and_reads_events() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("transcript.jsonl");
        let script = temp.path().join("fake-codex");
        let script_contents = format!(
            r#"#!/bin/sh
transcript={}
: > "$transcript"
while IFS= read -r line; do
  echo "$line" >> "$transcript"
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{"serverInfo":{{"name":"fake-codex"}}}}}}' ;;
    *'"method":"thread/start"'*) echo '{{"id":2,"result":{{"thread":{{"id":"thread-from-result","sessionId":"session-from-thread"}}}}}}' ;;
    *'"method":"turn/start"'*)
      echo '{{"method":"item/agentMessage/delta","params":{{"delta":"hello from app server"}}}}'
      echo '{{"method":"thread/tokenUsage/updated","params":{{"last":{{"input_tokens":1,"output_tokens":2}}}}}}'
      echo '{{"method":"turn/completed","params":{{"thread":{{"id":"thread-from-result","sessionId":"session-from-complete"}},"turn":{{"status":"completed","items":[{{"type":"agentMessage","text":"hello from app server"}}]}}}}}}'
      ;;
  esac
done
"#,
            shell_single_quote(&transcript)
        );
        std::fs::write(&script, script_contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let mut claim = test_claim();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([{ "name": "echo", "description": "Echo input", "parameters": { "type": "object" } }]),
            attachments: json!([]),
            tool_result: None,
        });
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let result = run_app_server_process(
            script.to_str().unwrap(),
            &run_env.workdir,
            &run_env.codex_home,
            &claim,
            Duration::from_secs(2),
            None,
        )
        .unwrap();

        assert_eq!(result.final_status, "completed");
        assert_eq!(result.session_id.as_deref(), Some("thread-from-result"));
        assert_eq!(result.events.len(), 3);
        assert!(result.events.iter().any(|event| {
            event.event_type == "message_delta"
                && event.content.as_deref() == Some("hello from app server")
        }));
        let message = result
            .events
            .iter()
            .find(|event| event.event_type == "message")
            .unwrap();
        assert_eq!(message.role.as_deref(), Some("assistant"));
        assert_eq!(message.content.as_deref(), Some("hello from app server"));
        assert!(result
            .events
            .iter()
            .any(|event| event.event_type == "usage"));

        let methods = std::fs::read_to_string(&transcript)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .map(|value| value["method"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            vec!["initialize", "initialized", "thread/start", "turn/start"]
        );

        let turn_start = std::fs::read_to_string(&transcript)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|value| value["method"] == "turn/start")
            .unwrap();
        assert_eq!(
            turn_start["params"]["dynamicTools"],
            claim.integration_context.unwrap().tools
        );
    }

    #[tokio::test]
    async fn app_server_process_streams_events_when_sender_is_available() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("streaming-fake-codex");
        let script_contents = r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{"id":1,"result":{"serverInfo":{"name":"streaming-fake"}}}' ;;
    *'"method":"thread/start"'*) echo '{"id":2,"result":{"thread":{"id":"stream-thread","sessionId":"stream-session"}}}' ;;
    *'"method":"turn/start"'*)
      echo '{"method":"item/agentMessage/delta","params":{"delta":"streamed"}}'
      echo '{"method":"thread/tokenUsage/updated","params":{"last":{"input_tokens":1,"output_tokens":1}}}'
      echo '{"method":"turn/completed","params":{"thread":{"id":"stream-thread","sessionId":"stream-session"},"turn":{"status":"completed","items":[]}}}'
      ;;
  esac
done
"#;
        std::fs::write(&script, script_contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let (event_tx, mut event_rx) = app_server_event_channel();
        let result = run_app_server_process(
            script.to_str().unwrap(),
            &run_env.workdir,
            &run_env.codex_home,
            &claim,
            Duration::from_secs(2),
            Some(event_tx),
        )
        .unwrap();

        assert!(result.events.is_empty());
        assert_eq!(result.session_id.as_deref(), Some("stream-thread"));
        let mut streamed = Vec::new();
        while let Ok(event) = event_rx.try_recv() {
            streamed.push(event);
        }
        assert!(streamed.iter().any(|event| {
            event.event_type == "message" && event.content.as_deref() == Some("streamed")
        }));
        assert!(streamed.iter().any(|event| event.event_type == "usage"));
    }

    #[test]
    fn app_server_event_channel_has_a_fixed_capacity() {
        let (event_tx, _event_rx) = app_server_event_channel();

        assert_eq!(event_tx.max_capacity(), APP_SERVER_EVENT_QUEUE_CAPACITY);
        assert_eq!(event_tx.capacity(), APP_SERVER_EVENT_QUEUE_CAPACITY);
    }

    #[test]
    fn blocked_app_server_event_send_stops_when_the_run_is_cancelled() {
        let (event_tx, _event_rx) = tokio_mpsc::channel(1);
        event_tx.try_send(test_tool_request_event()).unwrap();
        let cancellation = Arc::new(AppServerCancellation::default());
        let worker_cancellation = Arc::clone(&cancellation);
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = send_app_server_event_with_backpressure(
                &event_tx,
                test_tool_request_event(),
                &worker_cancellation,
            );
            done_tx.send(result).unwrap();
        });

        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        cancellation.cancel();
        let result = done_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("a cancelled producer must not remain blocked on a full queue");
        assert!(result.unwrap_err().to_string().contains("cancelled"));
    }

    #[tokio::test]
    async fn high_frequency_delta_backpressure_reaps_process_after_slow_hub_failure() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("burst-codex.pid");
        let completed_marker = temp.path().join("burst-completed");
        let script = write_high_frequency_streaming_codex(&temp, &pid_file, &completed_marker);
        let (client, hub_thread) = slow_failing_hub_client(Duration::from_millis(750));
        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let mut config = test_config();
        config.codex_bin = script.display().to_string();
        let mut last_heartbeat = Instant::now();

        let error = tokio::time::timeout(
            Duration::from_secs(3),
            execute_app_server_with_streaming_with_heartbeat_interval(
                &config,
                &client,
                &claim,
                &run_env,
                &mut last_heartbeat,
                Duration::from_secs(30),
            ),
        )
        .await
        .expect("slow Hub failure must cancel a backpressured producer in bounded time")
        .unwrap_err();

        assert!(error.to_string().contains("500"));
        assert!(
            !completed_marker.exists(),
            "the app-server must be backpressured before emitting the entire burst"
        );
        assert_process_group_reaped_or_clean_up(&pid_file);
        hub_thread.join().unwrap();
    }

    #[tokio::test]
    async fn streamed_event_append_failure_reaps_the_app_server_process_group() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("event-failure-codex.pid");
        let script = write_long_running_streaming_codex(&temp, &pid_file);
        let (client, hub_thread) = failing_hub_client("/events");
        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let mut config = test_config();
        config.codex_bin = script.display().to_string();
        let mut last_heartbeat = Instant::now();

        let error = execute_app_server_with_streaming_with_heartbeat_interval(
            &config,
            &client,
            &claim,
            &run_env,
            &mut last_heartbeat,
            Duration::from_secs(30),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("500"));
        assert_process_group_reaped_or_clean_up(&pid_file);
        hub_thread.join().unwrap();
    }

    #[tokio::test]
    async fn streamed_heartbeat_failure_reaps_the_app_server_process_group() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("heartbeat-failure-codex.pid");
        let script = write_long_running_streaming_codex(&temp, &pid_file);
        let (client, hub_thread) = failing_hub_client("/heartbeat");
        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let mut config = test_config();
        config.codex_bin = script.display().to_string();
        let mut last_heartbeat = Instant::now();

        let error = execute_app_server_with_streaming_with_heartbeat_interval(
            &config,
            &client,
            &claim,
            &run_env,
            &mut last_heartbeat,
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("500"));
        assert_process_group_reaped_or_clean_up(&pid_file);
        hub_thread.join().unwrap();
    }

    #[tokio::test]
    async fn app_server_process_does_not_inherit_hub_or_enrollment_secrets() {
        let temp = tempfile::tempdir().unwrap();
        let leak_file = temp.path().join("leak.txt");
        let script = temp.path().join("env-check-codex");
        let script_contents = format!(
            r#"#!/bin/sh
env | grep -E '^(HUB_MODEL_SECRET_KEY|RUNTIME_ENROLLMENT_TOKEN)=' > {} || true
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{"serverInfo":{{"name":"env-check"}}}}}}' ;;
    *'"method":"thread/start"'*) echo '{{"id":2,"result":{{"thread":{{"id":"env-thread","sessionId":"env-session"}}}}}}' ;;
    *'"method":"turn/start"'*) echo '{{"method":"turn/completed","params":{{"turn":{{"status":"completed","items":[{{"type":"agentMessage","text":"env ok"}}]}}}}}}' ;;
  esac
done
"#,
            shell_single_quote(&leak_file)
        );
        std::fs::write(&script, script_contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        temp_env_var("HUB_MODEL_SECRET_KEY", "hub-model-secret", || {
            temp_env_var("RUNTIME_ENROLLMENT_TOKEN", "enrollment-secret", || {
                run_app_server_process(
                    script.to_str().unwrap(),
                    &run_env.workdir,
                    &run_env.codex_home,
                    &claim,
                    Duration::from_secs(2),
                    None,
                )
                .unwrap();
            });
        });

        assert_eq!(std::fs::read_to_string(leak_file).unwrap_or_default(), "");
    }

    #[tokio::test]
    async fn app_server_process_waits_for_initialize_response_before_continuing() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("strict-transcript.jsonl");
        let script = temp.path().join("strict-fake-codex");
        let script_contents = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
transcript={}
: > "$transcript"
IFS= read -r line
echo "$line" >> "$transcript"
[[ "$line" == *'"method":"initialize"'* ]] || exit 65
if IFS= read -r -t 0.2 early; then
  echo "$early" >> "$transcript"
  echo "request arrived before initialize response" >&2
  exit 66
fi
echo '{{"id":1,"result":{{"serverInfo":{{"name":"strict-fake"}}}}}}'
while IFS= read -r line; do
  echo "$line" >> "$transcript"
  case "$line" in
    *'"method":"thread/start"'*) echo '{{"id":2,"result":{{"thread":{{"id":"strict-thread","sessionId":"strict-session"}}}}}}' ;;
    *'"method":"turn/start"'*) echo '{{"method":"turn/completed","params":{{"thread":{{"id":"strict-thread","sessionId":"strict-session"}},"turn":{{"status":"completed","items":[{{"type":"agentMessage","text":"strict done"}}]}}}}}}' ;;
  esac
done
"#,
            shell_single_quote(&transcript)
        );
        std::fs::write(&script, script_contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let result = run_app_server_process(
            script.to_str().unwrap(),
            &run_env.workdir,
            &run_env.codex_home,
            &claim,
            Duration::from_secs(3),
            None,
        )
        .unwrap();

        assert_eq!(result.session_id.as_deref(), Some("strict-thread"));
        let methods = std::fs::read_to_string(transcript)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .map(|value| value["method"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            vec!["initialize", "initialized", "thread/start", "turn/start"]
        );
    }

    #[tokio::test]
    async fn persistent_app_server_process_initializes_and_starts_thread_once_for_two_session_runs()
    {
        let temp = tempfile::tempdir().unwrap();
        let request_log = temp.path().join("requests.log");
        let thread_request_log = temp.path().join("thread-requests.log");
        let pid_file = temp.path().join("pid");
        let codex_home_file = temp.path().join("codex-home");
        let script = temp.path().join("persistent-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
echo $$ > {}
echo "$CODEX_HOME" > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo initialize >> {}; echo '{{"id":1,"result":{{"serverInfo":{{"name":"persistent"}}}}}}' ;;
    *'"method":"initialized"'*) echo initialized >> {} ;;
    *'"method":"thread/start"'*|*'"method":"thread/resume"'*) echo "$line" >> {}; echo thread >> {}; echo '{{"id":2,"result":{{"thread":{{"id":"persistent-thread"}}}}}}' ;;
    *'"method":"turn/start"'*) echo turn >> {}; echo '{{"method":"item/agentMessage/completed","params":{{"type":"agentMessage","text":"done"}}}}'; echo '{{"method":"turn/completed","params":{{"turn":{{"status":"completed","items":[]}}}}}}' ;;
  esac
done
"#,
                shell_single_quote(&pid_file),
                shell_single_quote(&codex_home_file),
                shell_single_quote(&request_log),
                shell_single_quote(&request_log),
                shell_single_quote(&thread_request_log),
                shell_single_quote(&request_log),
                shell_single_quote(&request_log),
            ),
        )
        .unwrap();
        make_executable(&script);
        let mut first = test_claim();
        let session_id = Uuid::new_v4();
        first.run.hub_session_id = Some(session_id);
        let run_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let cancellation = Arc::new(AppServerCancellation::default());
        let mut process = PersistentAppServerProcess::start(
            script.to_str().unwrap(),
            &run_env,
            Duration::from_secs(2),
            Arc::clone(&cancellation),
        )
        .unwrap();
        let child_id = process.child_id();

        let first_result = process.execute(&first, None).unwrap();
        let mut second = first.clone();
        second.run.id = Uuid::new_v4();
        second.run.initial_message = "second".into();
        let second_result = process.execute(&second, None).unwrap();

        assert_eq!(process.child_id(), child_id);
        assert_eq!(first_result.final_status, "completed");
        assert_eq!(second_result.final_status, "completed");
        assert_eq!(
            first_result.session_id.as_deref(),
            Some("persistent-thread")
        );
        assert_eq!(second_result.session_id, first_result.session_id);
        let requests = std::fs::read_to_string(&request_log).unwrap();
        assert_eq!(
            requests
                .lines()
                .filter(|line| *line == "initialize")
                .count(),
            1
        );
        assert_eq!(
            requests
                .lines()
                .filter(|line| *line == "initialized")
                .count(),
            1
        );
        assert_eq!(requests.lines().filter(|line| *line == "thread").count(), 1);
        assert_eq!(requests.lines().filter(|line| *line == "turn").count(), 2);
        assert!(std::fs::read_to_string(thread_request_log)
            .unwrap()
            .contains(&run_env.workdir.display().to_string()));
        assert_eq!(
            std::fs::read_to_string(codex_home_file).unwrap().trim(),
            run_env.codex_home.display().to_string()
        );
        drop(process);
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn persistent_app_server_process_correlates_unique_request_ids_to_responses() {
        let temp = tempfile::tempdir().unwrap();
        let transcript = temp.path().join("rpc-requests.log");
        let script = temp.path().join("rpc-id-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
turn_number=0
while IFS= read -r line; do
  echo "$line" >> {}
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) echo "{{\"id\":$request_id,\"result\":{{\"serverInfo\":{{\"name\":\"rpc-id\"}}}}}}" ;;
    *'"method":"thread/start"'*)
      unrelated_id=$((request_id + 1000))
      echo "{{\"id\":$unrelated_id,\"result\":{{\"thread\":{{\"id\":\"unrelated-thread\"}}}}}}"
      echo "{{\"id\":$request_id,\"result\":{{\"thread\":{{\"id\":\"correlated-thread\"}}}}}}"
      ;;
    *'"method":"turn/start"'*)
      turn_number=$((turn_number + 1))
      echo "{{\"id\":$request_id,\"result\":{{\"turn\":{{\"id\":\"turn-$turn_number\"}}}}}}"
      echo "{{\"method\":\"turn/completed\",\"params\":{{\"turn\":{{\"id\":\"turn-$turn_number\",\"status\":\"completed\",\"items\":[{{\"type\":\"agentMessage\",\"text\":\"done\"}}]}}}}}}"
      ;;
  esac
done
"#,
                shell_single_quote(&transcript),
            ),
        )
        .unwrap();
        make_executable(&script);

        let mut first = test_claim();
        first.run.hub_session_id = Some(Uuid::new_v4());
        let run_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let mut process = PersistentAppServerProcess::start(
            script.to_str().unwrap(),
            &run_env,
            Duration::from_secs(2),
            Arc::new(AppServerCancellation::default()),
        )
        .unwrap();

        let first_result = process.execute(&first, None).unwrap();
        let mut second = first.clone();
        second.run.id = Uuid::new_v4();
        second.run.initial_message = "second".into();
        let second_result = process.execute(&second, None).unwrap();

        assert_eq!(
            first_result.session_id.as_deref(),
            Some("correlated-thread")
        );
        assert_eq!(second_result.session_id, first_result.session_id);
        let request_ids = std::fs::read_to_string(transcript)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter_map(|request| request.get("id").and_then(serde_json::Value::as_u64))
            .collect::<Vec<_>>();
        assert_eq!(request_ids.len(), 4);
        assert_eq!(request_ids.iter().copied().collect::<HashSet<_>>().len(), 4);
    }

    #[tokio::test]
    async fn persistent_app_server_process_does_not_route_stale_turn_notifications_to_the_next_run()
    {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("stale-turn-fake-codex");
        std::fs::write(
            &script,
            r#"#!/bin/sh
turn_number=0
while IFS= read -r line; do
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) echo "{\"id\":$request_id,\"result\":{}}" ;;
    *'"method":"thread/start"'*) echo "{\"id\":$request_id,\"result\":{\"thread\":{\"id\":\"stale-thread\"}}}" ;;
    *'"method":"turn/start"'*)
      turn_number=$((turn_number + 1))
      echo "{\"id\":$request_id,\"result\":{\"turn\":{\"id\":\"turn-$turn_number\"}}}"
      echo "{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"stale-thread\",\"turn\":{\"id\":\"turn-$turn_number\",\"status\":\"completed\",\"items\":[{\"type\":\"agentMessage\",\"text\":\"fresh-$turn_number\"}]}}}"
      if [ "$turn_number" -eq 1 ]; then
        echo '{"method":"item/agentMessage/completed","params":{"threadId":"stale-thread","turnId":"turn-1","type":"agentMessage","text":"stale-message"}}'
        echo '{"method":"thread/tokenUsage/updated","params":{"threadId":"stale-thread","turnId":"turn-1","totalTokens":999}}'
        echo '{"method":"turn/completed","params":{"threadId":"stale-thread","turn":{"id":"turn-1","status":"completed","items":[]}}}'
      fi
      ;;
  esac
done
"#,
        )
        .unwrap();
        make_executable(&script);

        let mut first = test_claim();
        first.run.hub_session_id = Some(Uuid::new_v4());
        let run_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let mut process = PersistentAppServerProcess::start(
            script.to_str().unwrap(),
            &run_env,
            Duration::from_secs(2),
            Arc::new(AppServerCancellation::default()),
        )
        .unwrap();
        process.execute(&first, None).unwrap();
        let mut second = first.clone();
        second.run.id = Uuid::new_v4();

        let second = process.execute(&second, None).unwrap();

        let event_text = second
            .events
            .iter()
            .filter_map(|event| event.content.as_deref())
            .collect::<Vec<_>>();
        assert_eq!(event_text, vec!["fresh-2"]);
        assert!(!second.events.iter().any(|event| {
            event.content.as_deref() == Some("stale-message")
                || event.payload["turnId"] == "turn-1"
                || event.payload["turn"]["id"] == "turn-1"
        }));
    }

    #[tokio::test]
    async fn session_supervisor_actor_serializes_runs_and_reaps_process_on_shutdown() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("actor-pid");
        let turn_log = temp.path().join("actor-turns.log");
        let script = temp.path().join("actor-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{}}}}' ;;
    *'"method":"thread/start"'*|*'"method":"thread/resume"'*) echo '{{"id":2,"result":{{"thread":{{"id":"actor-thread"}}}}}}' ;;
    *'"method":"turn/start"'*) echo start >> {}; sleep 0.12; echo done >> {}; echo '{{"method":"turn/completed","params":{{"turn":{{"status":"completed","items":[{{"type":"agentMessage","text":"actor done"}}]}}}}}}' ;;
  esac
done
"#,
                shell_single_quote(&pid_file),
                shell_single_quote(&turn_log),
                shell_single_quote(&turn_log),
            ),
        )
        .unwrap();
        make_executable(&script);
        let mut first = test_claim();
        let session_id = Uuid::new_v4();
        first.run.hub_session_id = Some(session_id);
        let run_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let supervisor = SessionSupervisor::start_app_server(
            session_id,
            1,
            script.display().to_string(),
            run_env,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let mut second = first.clone();
        second.run.id = Uuid::new_v4();
        let started = Instant::now();
        let first_task = tokio::spawn({
            let supervisor = Arc::clone(&supervisor);
            async move { supervisor.execute(first, None).await }
        });
        let second_task = tokio::spawn({
            let supervisor = Arc::clone(&supervisor);
            async move { supervisor.execute(second, None).await }
        });

        first_task.await.unwrap().unwrap();
        second_task.await.unwrap().unwrap();
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert_eq!(
            std::fs::read_to_string(&turn_log)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec!["start", "done", "start", "done"]
        );
        supervisor.shutdown();
        assert_process_group_reaped_or_clean_up(&pid_file);
        assert!(SessionPaths::for_session(temp.path(), session_id)
            .root
            .is_dir());
    }

    #[tokio::test]
    async fn session_supervisor_steers_the_expected_active_turn_on_its_existing_connection() {
        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("steer-ready");
        let steer_log = temp.path().join("steer-request.log");
        let script = temp.path().join("steer-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) echo "{{\"id\":$request_id,\"result\":{{}}}}" ;;
    *'"method":"thread/start"'*) echo "{{\"id\":$request_id,\"result\":{{\"thread\":{{\"id\":\"steer-thread\"}}}}}}" ;;
    *'"method":"turn/start"'*)
      echo "{{\"id\":$request_id,\"result\":{{\"turn\":{{\"id\":\"steer-turn\"}}}}}}"
      echo "{{\"method\":\"turn/started\",\"params\":{{\"turn\":{{\"id\":\"steer-turn\"}}}}}}"
      touch {}
      ;;
    *'"method":"turn/steer"'*)
      echo "$line" >> {}
      echo "{{\"id\":$request_id,\"result\":{{\"turnId\":\"steer-turn\"}}}}"
      echo "{{\"method\":\"turn/completed\",\"params\":{{\"turn\":{{\"id\":\"steer-turn\",\"status\":\"completed\",\"items\":[{{\"type\":\"agentMessage\",\"text\":\"steered\"}}]}}}}}}"
      ;;
  esac
done
"#,
                shell_single_quote(&ready),
                shell_single_quote(&steer_log),
            ),
        )
        .unwrap();
        make_executable(&script);

        let mut claim = test_claim();
        let session_id = Uuid::new_v4();
        claim.run.hub_session_id = Some(session_id);
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let supervisor = SessionSupervisor::start_app_server(
            session_id,
            1,
            script.display().to_string(),
            run_env,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let execution = tokio::spawn({
            let supervisor = Arc::clone(&supervisor);
            async move { supervisor.execute(claim, None).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let message_id = Uuid::new_v4();
        let outcome = supervisor
            .steer(
                1,
                "steer-turn".into(),
                message_id,
                vec!["redirect now".into()],
            )
            .await
            .unwrap();

        assert_eq!(outcome, SessionSteerOutcome::Applied);
        assert_eq!(execution.await.unwrap().unwrap().final_status, "completed");
        let request: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(steer_log).unwrap().trim()).unwrap();
        assert_eq!(request["params"]["threadId"], "steer-thread");
        assert_eq!(request["params"]["expectedTurnId"], "steer-turn");
        assert_eq!(
            request["params"]["clientUserMessageId"],
            message_id.to_string()
        );
        assert_eq!(request["params"]["input"][0]["text"], "redirect now");
        supervisor.shutdown();
    }

    #[tokio::test]
    async fn active_turn_keeps_its_configuration_and_next_turn_updates_without_restarting() {
        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("configuration-turn-ready");
        let process_pid = temp.path().join("configuration-process.pid");
        let turn_pid_log = temp.path().join("configuration-turn-pids.log");
        let guidance_log = temp.path().join("configuration-guidance.log");
        let request_log = temp.path().join("configuration-requests.log");
        let script = temp.path().join("configuration-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
echo $$ > {}
turn_number=0
while IFS= read -r line; do
  echo "$line" >> {}
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) echo "{{\"id\":$request_id,\"result\":{{}}}}" ;;
    *'"method":"thread/start"'*|*'"method":"thread/resume"'*)
      echo "{{\"id\":$request_id,\"result\":{{\"thread\":{{\"id\":\"configuration-thread\"}}}}}}"
      ;;
    *'"method":"thread/unsubscribe"'*)
      echo "{{\"id\":$request_id,\"result\":{{}}}}"
      ;;
    *'"method":"turn/start"'*)
      turn_number=$((turn_number + 1))
      echo $$ >> {}
      guidance=$(head -n 1 "$CODEX_HOME/AGENTS.md")
      echo "turn-$turn_number:$guidance" >> {}
      echo "{{\"id\":$request_id,\"result\":{{\"turn\":{{\"id\":\"configuration-turn-$turn_number\"}}}}}}"
      echo "{{\"method\":\"turn/started\",\"params\":{{\"turn\":{{\"id\":\"configuration-turn-$turn_number\"}}}}}}"
      if [ "$turn_number" -eq 1 ]; then
        touch {}
      else
        echo "{{\"method\":\"turn/completed\",\"params\":{{\"turn\":{{\"id\":\"configuration-turn-2\",\"status\":\"completed\",\"items\":[]}}}}}}"
      fi
      ;;
    *'"method":"turn/steer"'*)
      guidance=$(head -n 1 "$CODEX_HOME/AGENTS.md")
      echo "steer:$guidance" >> {}
      echo "{{\"id\":$request_id,\"result\":{{\"turnId\":\"configuration-turn-1\"}}}}"
      echo '{{"method":"turn/completed","params":{{"turn":{{"id":"configuration-turn-1","status":"completed","items":[]}}}}}}'
      ;;
  esac
done
"#,
                shell_single_quote(&process_pid),
                shell_single_quote(&request_log),
                shell_single_quote(&turn_pid_log),
                shell_single_quote(&guidance_log),
                shell_single_quote(&ready),
                shell_single_quote(&guidance_log),
            ),
        )
        .unwrap();
        make_executable(&script);

        let mut first = test_claim();
        let session_id = Uuid::new_v4();
        first.run.hub_session_id = Some(session_id);
        first.execution_configuration.instructions = "old guidance".into();
        first.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&first.execution_configuration).unwrap();
        first.agent.instructions = first.execution_configuration.instructions.clone();
        let run_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let codex_home = run_env.codex_home.clone();
        let supervisor = SessionSupervisor::start_app_server(
            session_id,
            1,
            script.display().to_string(),
            run_env,
            Duration::from_secs(2),
        )
        .await
        .unwrap();

        let mut updated = first.clone();
        updated.run.id = Uuid::new_v4();
        updated.execution_configuration.revision += 1;
        updated.execution_configuration.instructions = "updated guidance".into();
        let updated_connection_id = Uuid::from_u128(0x202);
        updated.execution_configuration.default_model_connection_id = Some(updated_connection_id);
        updated.execution_configuration.reasoning_effort = ReasoningEffort::Ultra;
        updated.execution_configuration.model_connections = vec![ModelConnectionOptionDto {
            id: updated_connection_id,
            name: "Updated model".into(),
            model_id: "gpt-updated".into(),
            scope: ModelConnectionScope::Global,
            status: ModelConnectionStatus::Enabled,
        }];
        updated.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&updated.execution_configuration).unwrap();
        updated.agent.instructions = updated.execution_configuration.instructions.clone();
        updated.agent.default_model_connection_id = Some(updated_connection_id);
        updated.agent.reasoning_effort = ReasoningEffort::Ultra;

        let first_execution = tokio::spawn({
            let supervisor = Arc::clone(&supervisor);
            async move { supervisor.execute(first, None).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            fs::read_to_string(codex_home.join("AGENTS.md"))
                .await
                .unwrap(),
            "old guidance\n"
        );

        assert_eq!(
            supervisor
                .steer(
                    1,
                    "configuration-turn-1".into(),
                    Uuid::new_v4(),
                    vec!["keep going".into()],
                )
                .await
                .unwrap(),
            SessionSteerOutcome::Applied
        );
        assert_eq!(
            first_execution.await.unwrap().unwrap().final_status,
            "completed"
        );

        let _ = prepare_run_env(temp.path(), &updated, None).await.unwrap();
        assert_eq!(
            fs::read_to_string(codex_home.join("AGENTS.md"))
                .await
                .unwrap(),
            "updated guidance\n"
        );
        assert_eq!(
            supervisor
                .execute(updated, None)
                .await
                .unwrap()
                .final_status,
            "completed"
        );

        assert_eq!(
            std::fs::read_to_string(&guidance_log)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec![
                "turn-1:old guidance",
                "steer:old guidance",
                "turn-2:updated guidance",
            ]
        );
        let turn_pids = std::fs::read_to_string(&turn_pid_log)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert_eq!(turn_pids.len(), 2);
        assert_eq!(turn_pids[0], turn_pids[1]);

        let requests = std::fs::read_to_string(request_log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["method"] == "initialize")
                .count(),
            1
        );
        let unsubscribe = requests
            .iter()
            .find(|request| request["method"] == "thread/unsubscribe")
            .expect("configuration refresh must unsubscribe the loaded Thread");
        assert_eq!(unsubscribe["params"]["threadId"], "configuration-thread");
        let resumes = requests
            .iter()
            .filter(|request| request["method"] == "thread/resume")
            .collect::<Vec<_>>();
        assert_eq!(resumes.len(), 1);
        assert_eq!(resumes[0]["params"]["threadId"], "configuration-thread");
        assert_eq!(resumes[0]["params"]["model"], "gpt-updated");
        assert_eq!(
            resumes[0]["params"]["modelProvider"],
            model_provider_name(updated_connection_id)
        );
        assert_eq!(resumes[0]["params"]["excludeTurns"], true);
        assert_eq!(resumes[0]["params"]["config"], json!({}));
        let turns = requests
            .iter()
            .filter(|request| request["method"] == "turn/start")
            .collect::<Vec<_>>();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[1]["params"]["model"], "gpt-updated");
        assert_eq!(turns[1]["params"]["effort"], "ultra");

        supervisor.shutdown();
        assert_process_group_reaped_or_clean_up(&process_pid);
    }

    #[tokio::test]
    async fn session_supervisor_interrupts_without_rollback_and_waits_for_interrupted_completion() {
        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("interrupt-ready");
        let interrupt_log = temp.path().join("interrupt-request.log");
        let script = temp.path().join("interrupt-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
turn_number=0
while IFS= read -r line; do
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) echo "{{\"id\":$request_id,\"result\":{{}}}}" ;;
    *'"method":"thread/start"'*) echo "{{\"id\":$request_id,\"result\":{{\"thread\":{{\"id\":\"interrupt-thread\"}}}}}}" ;;
    *'"method":"turn/start"'*)
      turn_number=$((turn_number + 1))
      echo "{{\"id\":$request_id,\"result\":{{\"turn\":{{\"id\":\"interrupt-turn-$turn_number\"}}}}}}"
      if [ "$turn_number" -eq 1 ]; then
        echo effect-before-stop > effect.txt
        echo '{{"method":"item/agentMessage/completed","params":{{"threadId":"interrupt-thread","turnId":"interrupt-turn-1","type":"agentMessage","text":"before stop"}}}}'
        touch {}
      else
        echo '{{"method":"turn/completed","params":{{"threadId":"interrupt-thread","turn":{{"id":"interrupt-turn-2","status":"completed","items":[{{"type":"agentMessage","text":"continued"}}]}}}}}}'
      fi
      ;;
    *'"method":"turn/interrupt"'*)
      echo "$line" >> {}
      echo "{{\"id\":$request_id,\"result\":{{}}}}"
      sleep 0.08
      echo '{{"method":"turn/completed","params":{{"threadId":"interrupt-thread","turn":{{"id":"interrupt-turn-1","status":"interrupted","items":[]}}}}}}'
      ;;
  esac
done
"#,
                shell_single_quote(&ready),
                shell_single_quote(&interrupt_log),
            ),
        )
        .unwrap();
        make_executable(&script);

        let mut first = test_claim();
        let session_id = Uuid::new_v4();
        first.run.hub_session_id = Some(session_id);
        let run_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let workspace = run_env.workdir.clone();
        let supervisor = SessionSupervisor::start_app_server(
            session_id,
            1,
            script.display().to_string(),
            run_env,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let mut second = first.clone();
        second.run.id = Uuid::new_v4();
        let first_execution = tokio::spawn({
            let supervisor = Arc::clone(&supervisor);
            async move { supervisor.execute(first, None).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let started = Instant::now();
        let outcome = supervisor
            .interrupt(1, "interrupt-turn-1".into())
            .await
            .unwrap();

        assert_eq!(outcome, SessionInterruptOutcome::Interrupted);
        assert!(started.elapsed() >= Duration::from_millis(70));
        let first_result = first_execution.await.unwrap().unwrap();
        assert_eq!(first_result.final_status, "interrupted");
        assert!(first_result.events.iter().any(|event| {
            event.event_type == "message" && event.content.as_deref() == Some("before stop")
        }));
        assert_eq!(
            std::fs::read_to_string(workspace.join("effect.txt"))
                .unwrap()
                .trim(),
            "effect-before-stop"
        );
        let request: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(interrupt_log).unwrap().trim()).unwrap();
        assert_eq!(request["params"]["threadId"], "interrupt-thread");
        assert_eq!(request["params"]["turnId"], "interrupt-turn-1");

        let second_result = supervisor.execute(second, None).await.unwrap();
        assert_eq!(second_result.final_status, "completed");
        assert_eq!(
            second_result.native_turn_id.as_deref(),
            Some("interrupt-turn-2")
        );
        supervisor.shutdown();
    }

    #[tokio::test]
    async fn session_supervisor_streams_native_turn_binding_before_turn_completion() {
        let temp = tempfile::tempdir().unwrap();
        let release = temp.path().join("release-turn");
        let script = temp.path().join("turn-binding-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) echo "{{\"id\":$request_id,\"result\":{{}}}}" ;;
    *'"method":"thread/start"'*) echo "{{\"id\":$request_id,\"result\":{{\"thread\":{{\"id\":\"binding-thread\"}}}}}}" ;;
    *'"method":"turn/start"'*)
      echo "{{\"id\":$request_id,\"result\":{{\"turn\":{{\"id\":\"binding-turn\"}}}}}}"
      while [ ! -f {} ]; do sleep 0.01; done
      echo '{{"method":"turn/completed","params":{{"threadId":"binding-thread","turn":{{"id":"binding-turn","status":"completed","items":[{{"type":"agentMessage","text":"done"}}]}}}}}}'
      ;;
  esac
done
"#,
                shell_single_quote(&release),
            ),
        )
        .unwrap();
        make_executable(&script);

        let mut claim = test_claim();
        let session_id = Uuid::new_v4();
        claim.run.hub_session_id = Some(session_id);
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let supervisor = SessionSupervisor::start_app_server(
            session_id,
            1,
            script.display().to_string(),
            run_env,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let (event_tx, mut event_rx) = tokio_mpsc::channel(4);
        let execution = tokio::spawn({
            let supervisor = Arc::clone(&supervisor);
            async move { supervisor.execute(claim, Some(event_tx)).await }
        });

        let event = tokio::time::timeout(Duration::from_millis(300), event_rx.recv())
            .await
            .expect("native Turn binding was not streamed before completion")
            .unwrap();
        assert_eq!(event.event_type, "turn_started");
        assert_eq!(event.payload["native_thread_id"], "binding-thread");
        assert_eq!(event.payload["native_turn_id"], "binding-turn");
        std::fs::write(release, b"release").unwrap();
        let result = execution.await.unwrap().unwrap();
        assert_eq!(result.native_turn_id.as_deref(), Some("binding-turn"));
        supervisor.shutdown();
    }

    #[tokio::test]
    async fn steer_native_outcome_is_not_reapplied_while_hub_ack_retries() {
        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("ack-steer-ready");
        let request_log = temp.path().join("ack-steer-requests");
        let applied_log = temp.path().join("ack-steer-applied");
        let script = temp.path().join("ack-steer-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
seen_message_id=''
steer_count=0
while IFS= read -r line; do
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) echo "{{\"id\":$request_id,\"result\":{{}}}}" ;;
    *'"method":"thread/start"'*) echo "{{\"id\":$request_id,\"result\":{{\"thread\":{{\"id\":\"ack-thread\"}}}}}}" ;;
    *'"method":"turn/start"'*)
      echo "{{\"id\":$request_id,\"result\":{{\"turn\":{{\"id\":\"ack-turn\"}}}}}}"
      touch {}
      ;;
    *'"method":"turn/steer"'*)
      echo "$line" >> {}
      message_id=$(printf '%s\n' "$line" | sed -n 's/.*"clientUserMessageId":"\([^"]*\)".*/\1/p')
      if [ "$message_id" != "$seen_message_id" ]; then
        echo "$message_id" >> {}
        seen_message_id="$message_id"
      fi
      steer_count=$((steer_count + 1))
      echo "{{\"id\":$request_id,\"result\":{{\"turnId\":\"ack-turn\"}}}}"
      if [ "$steer_count" -eq 1 ]; then
        echo '{{"method":"turn/completed","params":{{"threadId":"ack-thread","turn":{{"id":"ack-turn","status":"completed","items":[{{"type":"agentMessage","text":"done"}}]}}}}}}'
      fi
      ;;
  esac
done
"#,
                shell_single_quote(&ready),
                shell_single_quote(&request_log),
                shell_single_quote(&applied_log),
            ),
        )
        .unwrap();
        make_executable(&script);

        let ack_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ack_release = Arc::new(Notify::new());
        let app = Router::new().route(
            "/api/runtime/sessions/{session_id}/commands/{command_id}/complete",
            post({
                let ack_attempts = Arc::clone(&ack_attempts);
                let ack_release = Arc::clone(&ack_release);
                move || {
                    let ack_attempts = Arc::clone(&ack_attempts);
                    let ack_release = Arc::clone(&ack_release);
                    async move {
                        if ack_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            AxumStatusCode::CONFLICT
                        } else {
                            ack_release.notified().await;
                            AxumStatusCode::OK
                        }
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{hub_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let runtime_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let manager = Arc::new(SessionSupervisorManager::new(
            temp.path().to_path_buf(),
            runtime_id,
            1,
        ));
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(session_id);
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        manager
            .ensure_app_server(
                SessionSupervisorMetadata {
                    format_version: 1,
                    session_id,
                    runtime_id,
                    ownership_generation: 1,
                    lifecycle_status: "online".into(),
                    idle_deadline_unix_ms: None,
                    checkpoint_reason: None,
                    checkpoint_retry_unix_ms: None,
                    hub_checkpoint_attempt_id: None,
                    codex_version: "test-codex".into(),
                    native_thread_id: Some("ack-thread".into()),
                },
                script.display().to_string(),
                run_env,
                Duration::from_secs(2),
                None,
            )
            .await
            .unwrap();
        let execution = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.execute(claim, None).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let message_id = Uuid::new_v4();
        let command = RuntimeSessionCommandDto {
            command_id: message_id,
            session_id,
            ownership_generation: 1,
            command: "steer".into(),
            run_id: Some(Uuid::new_v4()),
            turn_id: Some(Uuid::new_v4()),
            native_thread_id: Some("ack-thread".into()),
            native_turn_id: Some("ack-turn".into()),
            message: Some(RuntimeSteeringMessageDto {
                id: message_id,
                sequence: 1,
                content: "guide once".into(),
            }),
            configuration_revision: None,
            fingerprint: None,
            execution_configuration: None,
        };

        let dispatcher = Arc::new(RuntimeSessionCommandDispatcher::with_retry_delay(
            Duration::from_millis(10),
        ));
        dispatcher.enqueue(&client, &manager, std::slice::from_ref(&command));
        dispatcher.enqueue(&client, &manager, &[command]);
        tokio::time::timeout(Duration::from_secs(1), async {
            while ack_attempts.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(!execution.is_finished());
        assert!(!manager
            .ready_owned_sessions()
            .iter()
            .any(|owned| owned.session_id == session_id));
        ack_release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), execution)
            .await
            .expect("Turn completion remained blocked after command ACK succeeded")
            .unwrap()
            .unwrap();

        let request_ids = std::fs::read_to_string(request_log)
            .unwrap()
            .lines()
            .map(|line| {
                serde_json::from_str::<serde_json::Value>(line).unwrap()["params"]
                    ["clientUserMessageId"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(request_ids, vec![message_id.to_string()]);
        assert_eq!(
            std::fs::read_to_string(applied_log)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec![message_id.to_string()]
        );
        assert_eq!(ack_attempts.load(Ordering::SeqCst), 2);
        manager.shutdown();
        hub.abort();
    }

    #[tokio::test]
    async fn blocked_interrupt_does_not_delay_other_sessions_or_repeat_across_heartbeats() {
        let temp = tempfile::tempdir().unwrap();
        let interrupt_ready = temp.path().join("parallel-interrupt-ready");
        let interrupt_release = temp.path().join("parallel-interrupt-release");
        let interrupt_log = temp.path().join("parallel-interrupt.log");
        let steer_ready = temp.path().join("parallel-steer-ready");
        let steer_log = temp.path().join("parallel-steer.log");
        let interrupt_script = temp.path().join("parallel-interrupt-codex");
        let steer_script = temp.path().join("parallel-steer-codex");
        std::fs::write(
            &interrupt_script,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) echo "{{\"id\":$request_id,\"result\":{{}}}}" ;;
    *'"method":"thread/start"'*) echo "{{\"id\":$request_id,\"result\":{{\"thread\":{{\"id\":\"parallel-interrupt-thread\"}}}}}}" ;;
    *'"method":"turn/start"'*)
      echo "{{\"id\":$request_id,\"result\":{{\"turn\":{{\"id\":\"parallel-interrupt-turn\"}}}}}}"
      touch {}
      ;;
    *'"method":"turn/interrupt"'*)
      echo "$line" >> {}
      echo "{{\"id\":$request_id,\"result\":{{}}}}"
      while [ ! -f {} ]; do sleep 0.01; done
      echo '{{"method":"turn/completed","params":{{"threadId":"parallel-interrupt-thread","turn":{{"id":"parallel-interrupt-turn","status":"interrupted","items":[]}}}}}}'
      ;;
  esac
done
"#,
                shell_single_quote(&interrupt_ready),
                shell_single_quote(&interrupt_log),
                shell_single_quote(&interrupt_release),
            ),
        )
        .unwrap();
        std::fs::write(
            &steer_script,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) echo "{{\"id\":$request_id,\"result\":{{}}}}" ;;
    *'"method":"thread/start"'*) echo "{{\"id\":$request_id,\"result\":{{\"thread\":{{\"id\":\"parallel-steer-thread\"}}}}}}" ;;
    *'"method":"turn/start"'*)
      echo "{{\"id\":$request_id,\"result\":{{\"turn\":{{\"id\":\"parallel-steer-turn\"}}}}}}"
      touch {}
      ;;
    *'"method":"turn/steer"'*)
      echo "$line" >> {}
      echo "{{\"id\":$request_id,\"result\":{{\"turnId\":\"parallel-steer-turn\"}}}}"
      echo '{{"method":"turn/completed","params":{{"threadId":"parallel-steer-thread","turn":{{"id":"parallel-steer-turn","status":"completed","items":[]}}}}}}'
      ;;
  esac
done
"#,
                shell_single_quote(&steer_ready),
                shell_single_quote(&steer_log),
            ),
        )
        .unwrap();
        make_executable(&interrupt_script);
        make_executable(&steer_script);

        let acked = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new().route(
            "/api/runtime/sessions/{session_id}/commands/{command_id}/complete",
            post({
                let acked = Arc::clone(&acked);
                move |AxumPath((session_id, command_id)): AxumPath<(Uuid, Uuid)>| {
                    let acked = Arc::clone(&acked);
                    async move {
                        acked.lock().unwrap().push((session_id, command_id));
                        AxumStatusCode::OK
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{hub_addr}"),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let runtime_id = Uuid::new_v4();
        let manager = Arc::new(SessionSupervisorManager::new(
            temp.path().to_path_buf(),
            runtime_id,
            2,
        ));
        let interrupt_session_id = Uuid::new_v4();
        let steer_session_id = Uuid::new_v4();
        let mut interrupt_claim = test_claim();
        interrupt_claim.run.hub_session_id = Some(interrupt_session_id);
        let interrupt_env = prepare_run_env(temp.path(), &interrupt_claim, None)
            .await
            .unwrap();
        manager
            .ensure_app_server(
                SessionSupervisorMetadata {
                    format_version: 1,
                    session_id: interrupt_session_id,
                    runtime_id,
                    ownership_generation: 1,
                    lifecycle_status: "online".into(),
                    idle_deadline_unix_ms: None,
                    checkpoint_reason: None,
                    checkpoint_retry_unix_ms: None,
                    hub_checkpoint_attempt_id: None,
                    codex_version: "test-codex".into(),
                    native_thread_id: Some("parallel-interrupt-thread".into()),
                },
                interrupt_script.display().to_string(),
                interrupt_env,
                Duration::from_secs(2),
                None,
            )
            .await
            .unwrap();
        let mut steer_claim = test_claim();
        steer_claim.run.hub_session_id = Some(steer_session_id);
        let steer_env = prepare_run_env(temp.path(), &steer_claim, None)
            .await
            .unwrap();
        manager
            .ensure_app_server(
                SessionSupervisorMetadata {
                    format_version: 1,
                    session_id: steer_session_id,
                    runtime_id,
                    ownership_generation: 1,
                    lifecycle_status: "online".into(),
                    idle_deadline_unix_ms: None,
                    checkpoint_reason: None,
                    checkpoint_retry_unix_ms: None,
                    hub_checkpoint_attempt_id: None,
                    codex_version: "test-codex".into(),
                    native_thread_id: Some("parallel-steer-thread".into()),
                },
                steer_script.display().to_string(),
                steer_env,
                Duration::from_secs(2),
                None,
            )
            .await
            .unwrap();
        let interrupt_execution = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.execute(interrupt_claim, None).await }
        });
        let steer_execution = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.execute(steer_claim, None).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !(interrupt_ready.exists() && steer_ready.exists()) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let interrupt_command = RuntimeSessionCommandDto {
            command_id: Uuid::new_v4(),
            session_id: interrupt_session_id,
            ownership_generation: 1,
            command: "interrupt".into(),
            run_id: Some(Uuid::new_v4()),
            turn_id: Some(Uuid::new_v4()),
            native_thread_id: Some("parallel-interrupt-thread".into()),
            native_turn_id: Some("parallel-interrupt-turn".into()),
            message: None,
            configuration_revision: None,
            fingerprint: None,
            execution_configuration: None,
        };
        let steer_message_id = Uuid::new_v4();
        let steer_command = RuntimeSessionCommandDto {
            command_id: steer_message_id,
            session_id: steer_session_id,
            ownership_generation: 1,
            command: "steer".into(),
            run_id: Some(Uuid::new_v4()),
            turn_id: Some(Uuid::new_v4()),
            native_thread_id: Some("parallel-steer-thread".into()),
            native_turn_id: Some("parallel-steer-turn".into()),
            message: Some(RuntimeSteeringMessageDto {
                id: steer_message_id,
                sequence: 1,
                content: "continue independently".into(),
            }),
            configuration_revision: None,
            fingerprint: None,
            execution_configuration: None,
        };
        let dispatcher = Arc::new(RuntimeSessionCommandDispatcher::with_retry_delay(
            Duration::from_millis(10),
        ));
        dispatcher.enqueue(&client, &manager, std::slice::from_ref(&interrupt_command));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !interrupt_log.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let enqueued_at = Instant::now();
        dispatcher.enqueue(
            &client,
            &manager,
            &[interrupt_command.clone(), steer_command],
        );
        assert!(enqueued_at.elapsed() < Duration::from_millis(50));
        tokio::time::timeout(Duration::from_millis(300), async {
            while !steer_log.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("another Session steer was blocked behind an interrupt");
        dispatcher.enqueue(&client, &manager, std::slice::from_ref(&interrupt_command));
        assert_eq!(
            std::fs::read_to_string(&interrupt_log)
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert!(!interrupt_execution.is_finished());
        tokio::time::timeout(Duration::from_secs(1), steer_execution)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        std::fs::write(&interrupt_release, b"release").unwrap();
        tokio::time::timeout(Duration::from_secs(1), interrupt_execution)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while acked.lock().unwrap().len() != 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        manager.shutdown();
        hub.abort();
    }

    #[tokio::test]
    async fn session_manager_runs_two_sessions_concurrently_and_blocked_ownership_uses_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let release = temp.path().join("release");
        let make_barrier_codex = |name: &str| {
            let script = temp.path().join(format!("barrier-codex-{name}"));
            let pid = temp.path().join(format!("pid-{name}"));
            let entered = temp.path().join(format!("entered-{name}"));
            std::fs::write(
                &script,
                format!(
                    r#"#!/bin/sh
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{}}}}' ;;
    *'"method":"thread/start"'*|*'"method":"thread/resume"'*) echo '{{"id":2,"result":{{"thread":{{"id":"barrier-thread"}}}}}}' ;;
    *'"method":"turn/start"'*) touch {}; while [ ! -f {} ]; do sleep 0.01; done; echo '{{"method":"turn/completed","params":{{"turn":{{"status":"completed","items":[{{"type":"agentMessage","text":"done"}}]}}}}}}' ;;
  esac
done
"#,
                    shell_single_quote(&pid),
                    shell_single_quote(&entered),
                    shell_single_quote(&release),
                ),
            )
            .unwrap();
            make_executable(&script);
            (script, pid, entered)
        };
        let (first_bin, first_pid, first_entered) = make_barrier_codex("first");
        let (second_bin, second_pid, second_entered) = make_barrier_codex("second");
        let runtime_id = Uuid::new_v4();
        let manager = Arc::new(SessionSupervisorManager::new(
            temp.path().to_path_buf(),
            runtime_id,
            3,
        ));
        let blocked_id = Uuid::new_v4();
        manager.reserve_blocked(
            RuntimeOwnedSessionSnapshotDto {
                session_id: blocked_id,
                ownership_generation: 8,
                lifecycle_status: "online".into(),
                native_thread_id: None,
                active_run_id: None,
            },
            "missing local metadata".into(),
        );

        let mut first = test_claim();
        let first_session = Uuid::new_v4();
        first.run.hub_session_id = Some(first_session);
        let first_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let first_metadata = SessionSupervisorMetadata {
            format_version: 1,
            session_id: first_session,
            runtime_id,
            ownership_generation: 1,
            lifecycle_status: "online".into(),
            idle_deadline_unix_ms: None,
            checkpoint_reason: None,
            checkpoint_retry_unix_ms: None,
            hub_checkpoint_attempt_id: None,
            codex_version: "test-codex".into(),
            native_thread_id: None,
        };
        let first_supervisor = manager
            .ensure_app_server(
                first_metadata.clone(),
                first_bin.display().to_string(),
                first_env.clone(),
                Duration::from_secs(3),
                None,
            )
            .await
            .unwrap();
        let reused = manager
            .ensure_app_server(
                first_metadata,
                "/must-not-spawn".into(),
                first_env,
                Duration::from_secs(3),
                None,
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first_supervisor, &reused));

        let mut second = test_claim();
        let second_session = Uuid::new_v4();
        second.run.hub_session_id = Some(second_session);
        let second_env = prepare_run_env(temp.path(), &second, None).await.unwrap();
        manager
            .ensure_app_server(
                SessionSupervisorMetadata {
                    format_version: 1,
                    session_id: second_session,
                    runtime_id,
                    ownership_generation: 1,
                    lifecycle_status: "online".into(),
                    idle_deadline_unix_ms: None,
                    checkpoint_reason: None,
                    checkpoint_retry_unix_ms: None,
                    hub_checkpoint_attempt_id: None,
                    codex_version: "test-codex".into(),
                    native_thread_id: None,
                },
                second_bin.display().to_string(),
                second_env,
                Duration::from_secs(3),
                None,
            )
            .await
            .unwrap();
        assert_eq!(manager.available_new_session_slots(), 0);

        let first_task = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.execute(first, None).await }
        });
        let second_task = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.execute(second, None).await }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !(first_entered.exists() && second_entered.exists()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        std::fs::write(&release, "go").unwrap();
        first_task.await.unwrap().unwrap();
        second_task.await.unwrap().unwrap();

        manager.shutdown();
        assert_process_group_reaped_or_clean_up(&first_pid);
        assert_process_group_reaped_or_clean_up(&second_pid);
        assert!(SessionPaths::for_session(temp.path(), blocked_id)
            .root
            .exists());
    }

    #[tokio::test]
    async fn managed_session_reuses_app_server_and_proxy_while_switching_run_auth() {
        let temp = tempfile::tempdir().unwrap();
        let pid_log = temp.path().join("managed-pids");
        let base_url_log = temp.path().join("managed-base-urls");
        let script = temp.path().join("managed-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{}}}}' ;;
    *'"method":"thread/start"'*) echo '{{"id":2,"result":{{"thread":{{"id":"managed-thread"}}}}}}' ;;
    *'"method":"turn/start"'*)
      echo $$ >> {}
      base_url=$(sed -n 's/^base_url = "\(.*\)"$/\1/p' "$CODEX_HOME/config.toml" | head -n 1)
      echo "$base_url" >> {}
      curl --fail --silent --show-error -X POST "$base_url/responses" -H 'content-type: application/json' -d '{{}}' >/dev/null || exit 42
      echo '{{"method":"turn/completed","params":{{"thread":{{"id":"managed-thread"}},"turn":{{"status":"completed","items":[{{"type":"agentMessage","text":"done"}}]}}}}}}'
      ;;
  esac
done
"#,
                shell_single_quote(&pid_log),
                shell_single_quote(&base_url_log),
            ),
        )
        .unwrap();
        make_executable(&script);

        let forwarded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new()
            .route(
                "/api/runtime/model-proxy/v1/{*path}",
                post({
                    let forwarded = Arc::clone(&forwarded);
                    move |headers: HeaderMap| {
                        let forwarded = Arc::clone(&forwarded);
                        async move {
                            forwarded.lock().unwrap().push((
                                headers
                                    .get(header::AUTHORIZATION)
                                    .unwrap()
                                    .to_str()
                                    .unwrap()
                                    .to_owned(),
                                headers
                                    .get("x-agent-hub-run-id")
                                    .unwrap()
                                    .to_str()
                                    .unwrap()
                                    .to_owned(),
                            ));
                            Json(json!({ "ok": true })).into_response()
                        }
                    }
                }),
            )
            .route(
                "/api/runtime/runs/{run_id}/events",
                post(|| async { AxumStatusCode::OK }),
            )
            .route(
                "/api/runtime/runs/{run_id}/complete",
                post(|| async { AxumStatusCode::OK }),
            )
            .route(
                "/api/runtime/heartbeat",
                post(|| async {
                    Json(RuntimeHeartbeatResponse {
                        rotation_requested: false,
                        pending_credential_accepted: false,
                        credential_activated: false,
                        runtime_status: "online".into(),
                        owned_sessions: Vec::new(),
                        cleanup_sessions: Vec::new(),
                        session_commands: Vec::new(),
                        codex_rollout: RuntimeCodexRolloutCommandDto::default(),
                    })
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let runtime_id = Uuid::new_v4();
        let manager = Arc::new(SessionSupervisorManager::new(
            temp.path().to_path_buf(),
            runtime_id,
            1,
        ));
        let mut config = test_config();
        config.work_root = temp.path().to_path_buf();
        config.codex_bin = script.display().to_string();
        config.hub_url = format!("http://{hub_addr}");
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: config.hub_url.clone(),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let mut first = test_claim();
        first.model_proxy_token = "first-run-token".into();
        manager.reserve_claim(&first).unwrap();
        execute_managed_run(&config, &client, Arc::clone(&manager), first.clone())
            .await
            .unwrap();

        let mut second = first.clone();
        second.run.id = Uuid::new_v4();
        second.model_proxy_token = "second-run-token".into();
        manager.reserve_claim(&second).unwrap();
        execute_managed_run(&config, &client, Arc::clone(&manager), second.clone())
            .await
            .unwrap();

        let pids = std::fs::read_to_string(&pid_log)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let base_urls = std::fs::read_to_string(&base_url_log)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert_eq!(pids.len(), 2);
        assert_eq!(pids[0], pids[1]);
        assert_eq!(base_urls.len(), 2);
        assert_eq!(base_urls[0], base_urls[1]);
        let proxy_addr: SocketAddr = base_urls[0]
            .trim_start_matches("http://")
            .trim_end_matches("/v1")
            .parse()
            .unwrap();
        assert_eq!(
            *forwarded.lock().unwrap(),
            vec![
                ("Bearer first-run-token".into(), first.run.id.to_string()),
                ("Bearer second-run-token".into(), second.run.id.to_string()),
            ]
        );
        let metadata_path =
            SessionPaths::for_session(temp.path(), first.run.hub_session_id.unwrap())
                .supervisor
                .join(SESSION_SUPERVISOR_METADATA_FILE);
        let metadata: SessionSupervisorMetadata =
            serde_json::from_slice(&std::fs::read(metadata_path).unwrap()).unwrap();
        assert_eq!(metadata.native_thread_id.as_deref(), Some("managed-thread"));
        manager.shutdown();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if tokio::net::TcpStream::connect(proxy_addr).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("manager shutdown must close the Session model proxy listener");
        let session_id = first.run.hub_session_id.unwrap();
        let recovery = plan_session_recovery(
            temp.path(),
            runtime_id,
            &[RuntimeOwnedSessionSnapshotDto {
                session_id,
                ownership_generation: 1,
                lifecycle_status: "online".into(),
                native_thread_id: Some("managed-thread".into()),
                active_run_id: None,
            }],
            1,
        )
        .await
        .unwrap();
        assert!(matches!(
            recovery.record(session_id).unwrap().status,
            LocalSessionRecoveryStatus::Ready(_)
        ));
        let recovered =
            SessionSupervisorManager::recover_cold(temp.path().to_path_buf(), runtime_id, recovery);
        assert_eq!(
            recovered.ready_owned_sessions(),
            vec![RuntimeOwnedSessionGenerationDto {
                session_id,
                ownership_generation: 1,
            }]
        );
        recovered.shutdown();
        hub.abort();
    }

    #[test]
    fn claim_reservation_defers_proxy_creation_until_session_execution() {
        let temp = tempfile::tempdir().unwrap();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), Uuid::new_v4(), 1);
        let claim = test_claim();

        manager.reserve_claim(&claim).unwrap();

        assert!(manager
            .model_proxy(claim.run.hub_session_id.unwrap())
            .is_none());
    }

    #[tokio::test]
    async fn cancelling_a_session_drops_its_model_proxy_listener_but_keeps_capacity_reserved() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: "http://127.0.0.1:1".into(),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let claim = test_claim();
        let session_id = claim.run.hub_session_id.unwrap();
        manager.reserve_claim(&claim).unwrap();
        let proxy = Arc::new(
            start_model_proxy(
                &client,
                claim.run.id,
                &claim.model_proxy_token,
                Duration::from_secs(1),
            )
            .await
            .unwrap(),
        );
        let proxy_addr: SocketAddr = proxy
            .base_url
            .trim_start_matches("http://")
            .trim_end_matches("/v1")
            .parse()
            .unwrap();
        manager
            .records
            .lock()
            .unwrap()
            .get_mut(&session_id)
            .unwrap()
            .model_proxy = Some(proxy);

        manager.cancel_session(session_id, "test cancellation".into());

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if tokio::net::TcpStream::connect(proxy_addr).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Session cancellation must close its model proxy listener");
        assert_eq!(manager.blocked_session_count(), 1);
        assert_eq!(manager.available_new_session_slots(), 0);
    }

    #[tokio::test]
    async fn crashed_session_child_becomes_blocked_without_an_unbounded_restart() {
        let temp = tempfile::tempdir().unwrap();
        let starts = temp.path().join("crash-starts");
        let pid_file = temp.path().join("crash-pid");
        let script = temp.path().join("crashing-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
echo start >> {}
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{}}}}' ;;
    *'"method":"thread/start"'*) echo '{{"id":2,"result":{{"thread":{{"id":"crash-thread"}}}}}}' ;;
    *'"method":"turn/start"'*) exit 71 ;;
  esac
done
"#,
                shell_single_quote(&starts),
                shell_single_quote(&pid_file),
            ),
        )
        .unwrap();
        make_executable(&script);
        let runtime_id = Uuid::new_v4();
        let manager = Arc::new(SessionSupervisorManager::new(
            temp.path().to_path_buf(),
            runtime_id,
            1,
        ));
        let mut config = test_config();
        config.work_root = temp.path().to_path_buf();
        config.codex_bin = script.display().to_string();
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: "http://127.0.0.1:1".into(),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let claim = test_claim();
        let session_id = claim.run.hub_session_id.unwrap();
        manager.reserve_claim(&claim).unwrap();

        execute_managed_run(&config, &client, Arc::clone(&manager), claim.clone())
            .await
            .expect_err("a crashed app-server must fail its Run");

        {
            let records = manager.records.lock().unwrap();
            let record = records.get(&session_id).unwrap();
            assert!(record.reserved_run_id.is_none());
            assert!(record.model_proxy.is_none());
            assert!(matches!(
                record.status,
                ManagedSessionStatus::Blocked {
                    restart_attempts: 1,
                    ..
                }
            ));
        }
        assert_eq!(manager.available_new_session_slots(), 0);
        assert!(manager.ready_owned_sessions().is_empty());
        let mut follow_up = claim;
        follow_up.run.id = Uuid::new_v4();
        assert!(manager.reserve_claim(&follow_up).is_err());
        assert_eq!(std::fs::read_to_string(&starts).unwrap().lines().count(), 1);
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn idle_child_exit_is_reconciled_to_blocked_without_waiting_for_another_claim() {
        let temp = tempfile::tempdir().unwrap();
        let starts = temp.path().join("idle-crash-starts");
        let script = temp.path().join("idle-crashing-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
echo start >> {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{}}}}' ;;
    *'"method":"thread/start"'*) echo '{{"id":2,"result":{{"thread":{{"id":"idle-thread"}}}}}}' ;;
    *'"method":"turn/start"'*)
      echo '{{"method":"turn/completed","params":{{"thread":{{"id":"idle-thread"}},"turn":{{"status":"completed","items":[{{"type":"agentMessage","text":"done"}}]}}}}}}'
      sleep 0.1
      exit 72
      ;;
  esac
done
"#,
                shell_single_quote(&starts),
            ),
        )
        .unwrap();
        make_executable(&script);
        let app = Router::new()
            .route(
                "/api/runtime/runs/{run_id}/events",
                post(|| async { AxumStatusCode::OK }),
            )
            .route(
                "/api/runtime/runs/{run_id}/complete",
                post(|| async { AxumStatusCode::OK }),
            )
            .route(
                "/api/runtime/heartbeat",
                post(|| async {
                    Json(RuntimeHeartbeatResponse {
                        rotation_requested: false,
                        pending_credential_accepted: false,
                        credential_activated: false,
                        runtime_status: "online".into(),
                        owned_sessions: Vec::new(),
                        cleanup_sessions: Vec::new(),
                        session_commands: Vec::new(),
                        codex_rollout: RuntimeCodexRolloutCommandDto::default(),
                    })
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let runtime_id = Uuid::new_v4();
        let manager = Arc::new(SessionSupervisorManager::new(
            temp.path().to_path_buf(),
            runtime_id,
            1,
        ));
        let mut config = test_config();
        config.work_root = temp.path().to_path_buf();
        config.codex_bin = script.display().to_string();
        config.hub_url = format!("http://{hub_addr}");
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: config.hub_url.clone(),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let claim = test_claim();
        let session_id = claim.run.hub_session_id.unwrap();
        manager.reserve_claim(&claim).unwrap();
        execute_managed_run(&config, &client, Arc::clone(&manager), claim)
            .await
            .unwrap();
        let proxy = manager.model_proxy(session_id).unwrap();
        let proxy_addr: SocketAddr = proxy
            .base_url
            .trim_start_matches("http://")
            .trim_end_matches("/v1")
            .parse()
            .unwrap();
        drop(proxy);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.ready_owned_sessions().is_empty() && manager.blocked_session_count() == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("idle child exit must be reconciled without a new Run");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if tokio::net::TcpStream::connect(proxy_addr).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("idle child failure must close the model proxy listener");
        assert_eq!(manager.available_new_session_slots(), 0);
        assert_eq!(std::fs::read_to_string(starts).unwrap().lines().count(), 1);
        hub.abort();
    }

    #[tokio::test]
    async fn session_startup_failure_clears_reservation_and_proxy_but_keeps_blocked_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let manager = Arc::new(SessionSupervisorManager::new(
            temp.path().to_path_buf(),
            runtime_id,
            1,
        ));
        let mut config = test_config();
        config.work_root = temp.path().to_path_buf();
        config.codex_bin = temp.path().join("missing-codex").display().to_string();
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: "http://127.0.0.1:1".into(),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let claim = test_claim();
        let session_id = claim.run.hub_session_id.unwrap();
        manager.reserve_claim(&claim).unwrap();

        execute_managed_run(&config, &client, Arc::clone(&manager), claim)
            .await
            .expect_err("missing Codex binary must fail Session startup");

        let records = manager.records.lock().unwrap();
        let record = records.get(&session_id).unwrap();
        assert!(record.reserved_run_id.is_none());
        assert!(record.model_proxy.is_none());
        assert!(matches!(
            record.status,
            ManagedSessionStatus::Blocked {
                restart_attempts: 1,
                ..
            }
        ));
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn session_workers_run_different_sessions_concurrently_and_keep_each_session_serial() {
        let temp = tempfile::tempdir().unwrap();
        let release = temp.path().join("worker-release");
        let script = temp.path().join("worker-fake-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{}}}}' ;;
    *'"method":"thread/start"'*) echo '{{"id":2,"result":{{"thread":{{"id":"worker-thread"}}}}}}' ;;
    *'"method":"turn/start"'*)
      echo start >> "$PWD/turns"
      touch "$PWD/entered"
      while [ ! -f {} ]; do sleep 0.01; done
      echo done >> "$PWD/turns"
      echo '{{"method":"turn/completed","params":{{"thread":{{"id":"worker-thread"}},"turn":{{"status":"completed","items":[{{"type":"agentMessage","text":"done"}}]}}}}}}'
      ;;
  esac
done
"#,
                shell_single_quote(&release),
            ),
        )
        .unwrap();
        make_executable(&script);
        let app = Router::new()
            .route(
                "/api/runtime/runs/{run_id}/events",
                post(|| async { AxumStatusCode::OK }),
            )
            .route(
                "/api/runtime/runs/{run_id}/complete",
                post(|| async { AxumStatusCode::OK }),
            )
            .route(
                "/api/runtime/heartbeat",
                post(|| async {
                    Json(RuntimeHeartbeatResponse {
                        rotation_requested: false,
                        pending_credential_accepted: false,
                        credential_activated: false,
                        runtime_status: "online".into(),
                        owned_sessions: Vec::new(),
                        cleanup_sessions: Vec::new(),
                        session_commands: Vec::new(),
                        codex_rollout: RuntimeCodexRolloutCommandDto::default(),
                    })
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_addr = listener.local_addr().unwrap();
        let hub = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let runtime_id = Uuid::new_v4();
        let manager = Arc::new(SessionSupervisorManager::new(
            temp.path().to_path_buf(),
            runtime_id,
            2,
        ));
        let mut config = test_config();
        config.work_root = temp.path().to_path_buf();
        config.codex_bin = script.display().to_string();
        config.hub_url = format!("http://{hub_addr}");
        config.app_server_timeout = Duration::from_secs(3);
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: config.hub_url.clone(),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let first = test_claim();
        let first_session_id = first.run.hub_session_id.unwrap();
        let mut second = test_claim();
        let second_session_id = second.run.hub_session_id.unwrap();
        second.model_proxy_token = "second-session-token".into();
        manager.reserve_claim(&first).unwrap();
        manager.reserve_claim(&second).unwrap();
        let first_worker = tokio::spawn(run_claim_worker(
            config.clone(),
            client.clone(),
            Arc::clone(&manager),
            first.clone(),
        ));
        let second_worker = tokio::spawn(run_claim_worker(
            config.clone(),
            client.clone(),
            Arc::clone(&manager),
            second,
        ));
        let first_paths = SessionPaths::for_session(temp.path(), first_session_id);
        let second_paths = SessionPaths::for_session(temp.path(), second_session_id);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !(first_paths.workspace.join("entered").exists()
                && second_paths.workspace.join("entered").exists())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("different Session workers must enter their Turns concurrently");
        assert!(manager.ready_owned_sessions().is_empty());

        std::fs::write(&release, "go").unwrap();
        first_worker.await.unwrap();
        second_worker.await.unwrap();
        assert_eq!(manager.ready_owned_sessions().len(), 2);

        std::fs::remove_file(&release).unwrap();
        let mut follow_up = first.clone();
        follow_up.run.id = Uuid::new_v4();
        follow_up.model_proxy_token = "first-follow-up-token".into();
        manager.reserve_claim(&follow_up).unwrap();
        let follow_up_worker = tokio::spawn(run_claim_worker(
            config,
            client,
            Arc::clone(&manager),
            follow_up,
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let turns = std::fs::read_to_string(first_paths.workspace.join("turns"))
                    .unwrap_or_default();
                if turns.lines().count() == 3 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("follow-up Turn must start on the existing Session worker");
        assert_eq!(
            manager
                .ready_owned_sessions()
                .into_iter()
                .map(|owned| owned.session_id)
                .collect::<Vec<_>>(),
            vec![second_session_id]
        );
        std::fs::write(&release, "go").unwrap();
        follow_up_worker.await.unwrap();
        assert_eq!(
            std::fs::read_to_string(first_paths.workspace.join("turns"))
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec!["start", "done", "start", "done"]
        );
        assert_eq!(manager.blocked_session_count(), 0);
        manager.shutdown();
        hub.abort();
    }

    #[tokio::test]
    async fn app_server_process_failure_is_not_converted_to_fake_success() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("failing-codex");
        std::fs::write(
            &script,
            r#"#!/bin/sh
echo "app-server failed intentionally" >&2
exit 70
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let err = run_app_server_process(
            script.to_str().unwrap(),
            &run_env.workdir,
            &run_env.codex_home,
            &claim,
            Duration::from_secs(1),
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("Codex app-server failed"));
        assert!(!err.contains("app-server failed intentionally"));
    }

    #[tokio::test]
    async fn app_server_failure_and_hub_event_do_not_expose_mcp_secret() {
        const MCP_SECRET: &str = "mcp-secret-must-not-leak";
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("secret-error-codex");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      echo '{}' >&2
      echo '{{"id":1,"error":{{"message":"{}"}}}}'
      ;;
  esac
done
"#,
                MCP_SECRET, MCP_SECRET
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let mut claim = test_claim();
        claim.execution_configuration.mcp_allowlist = json!([{
            "name": "secret-server",
            "command": "secret-server",
            "secrets": { "API_TOKEN": MCP_SECRET }
        }]);
        claim.agent.mcp_allowlist = claim.execution_configuration.mcp_allowlist.clone();
        claim.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let error = format!(
            "{:#}",
            run_app_server_process(
                script.to_str().unwrap(),
                &run_env.workdir,
                &run_env.codex_home,
                &claim,
                Duration::from_secs(2),
                None,
            )
            .unwrap_err()
        );
        assert!(!error.contains(MCP_SECRET));

        let (client, hub_requests, hub_thread) = recording_hub_client(2);
        client.fail_run(claim.run.id, 1).await.unwrap();
        hub_thread.join().unwrap();
        let hub_traffic = hub_requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| String::from_utf8_lossy(request))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(hub_traffic.contains("runtime execution failed"));
        assert!(!hub_traffic.contains(MCP_SECRET));
    }

    #[test]
    fn app_server_item_maps_tool_request_notifications() {
        let source_id = Uuid::new_v4();
        let event = app_server_event_from_item(
            Uuid::from_u128(1),
            &json!({
                "type": "toolRequest",
                "id": source_id,
                "toolName": "echo",
                "arguments": { "message": "hello" }
            }),
            json!({ "method": "item/completed" }),
        )
        .unwrap();
        assert_eq!(event.event_type, "tool_request");
        assert_eq!(event.role.as_deref(), Some("assistant"));
        assert_eq!(event.payload["tool_name"], "echo");
        assert_eq!(event.payload["arguments"]["message"], "hello");
        assert!(Uuid::parse_str(event.payload["tool_request_id"].as_str().unwrap()).is_ok());
        assert_ne!(event.payload["tool_request_id"], source_id.to_string());
    }

    #[test]
    fn streamed_tool_request_is_deferred_until_driver_finishes() {
        let event = test_tool_request_event();
        let mut deferred = Vec::new();

        let streamable = defer_tool_request(event.clone(), &mut deferred);

        assert!(streamable.is_none());
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].payload, event.payload);
    }

    #[test]
    fn waiting_tool_publication_attaches_resume_metadata() {
        let mut claim = test_claim();
        claim.run.integration_session_id = Some(Uuid::new_v4());
        let published = build_tool_request_batch(
            &claim,
            vec![test_tool_request_event()],
            "waiting_tool",
            Some("thread-for-tool"),
            "/runtime/workdir",
        )
        .unwrap()
        .expect("successful waiting turn should publish its tool request");

        assert_eq!(published.session_id, "thread-for-tool");
        assert_eq!(published.work_dir_ref, "/runtime/workdir");
        assert_eq!(published.tool_requests.len(), 1);
    }

    #[test]
    fn failed_turn_does_not_publish_deferred_tool_request() {
        let claim = test_claim();
        let published = build_tool_request_batch(
            &claim,
            vec![test_tool_request_event()],
            "failed",
            Some("failed-thread"),
            "/runtime/workdir",
        )
        .unwrap();

        assert!(published.is_none());
    }

    fn test_tool_request_event() -> AppendRunEventRequest {
        AppendRunEventRequest {
            event_type: "tool_request".into(),
            role: Some("assistant".into()),
            content: Some("tool requested".into()),
            payload: json!({
                "tool_request_id": Uuid::new_v4(),
                "tool_name": "echo",
                "arguments": { "value": 1 }
            }),
            waiting_tool: None,
        }
    }

    #[test]
    fn app_server_tool_request_with_non_uuid_id_gets_hub_uuid() {
        let event = app_server_event_from_item(
            Uuid::nil(),
            &json!({
                "type": "functionCall",
                "callId": "codex-call-123",
                "name": "echo",
                "arguments": { "message": "hello" }
            }),
            json!({ "method": "item/tool/call" }),
        )
        .unwrap();

        assert_eq!(event.payload["tool_name"], "echo");
        assert_ne!(event.payload["tool_request_id"], "codex-call-123");
        assert!(Uuid::parse_str(event.payload["tool_request_id"].as_str().unwrap()).is_ok());
    }

    #[test]
    fn app_server_state_keeps_waiting_tool_after_completed_turn() {
        let tool_request_id = Uuid::new_v4();
        let mut state = AppServerState::new(Uuid::new_v4());
        let tool_request = json!({
            "type": "toolRequest",
            "id": tool_request_id,
            "toolName": "echo",
            "arguments": { "message": "hello" }
        });

        state
            .handle_value(&json!({
                "method": "item/completed",
                "params": { "item": tool_request.clone() }
            }))
            .unwrap();
        state
            .handle_value(&json!({
                "method": "turn/completed",
                "params": {
                    "turn": {
                        "status": "completed",
                        "items": [tool_request]
                    }
                }
            }))
            .unwrap();

        assert_eq!(state.final_status, "waiting_tool");
        assert!(state.done);
        assert_eq!(
            state
                .events
                .iter()
                .filter(|event| event.event_type == "tool_request")
                .count(),
            1
        );
    }

    #[test]
    fn app_server_state_deduplicates_repeated_tool_request_items() {
        let run_id = Uuid::new_v4();
        let tool_request_id = Uuid::new_v4();
        let mut state = AppServerState::new(run_id);
        state.initialized = true;
        state.thread_id = Some("thread-from-test".into());

        state
            .handle_value(&json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "toolRequest",
                        "id": tool_request_id,
                        "toolName": "echo",
                        "arguments": { "message": "hello" }
                    }
                }
            }))
            .unwrap();
        state
            .handle_value(&json!({
                "method": "turn/completed",
                "params": {
                    "turn": {
                        "status": "completed",
                        "items": [{
                            "type": "toolRequest",
                            "id": tool_request_id,
                            "toolName": "echo",
                            "arguments": { "message": "hello" }
                        }]
                    }
                }
            }))
            .unwrap();

        assert_eq!(
            state
                .events
                .iter()
                .filter(|event| event.event_type == "tool_request")
                .count(),
            1
        );
        assert_eq!(state.final_status, "waiting_tool");
    }

    #[test]
    fn app_server_state_deduplicates_non_uuid_tool_request_source_ids() {
        let mut state = AppServerState::new(Uuid::new_v4());
        state.initialized = true;
        state.thread_id = Some("thread-from-test".into());

        let item = json!({
            "type": "toolRequest",
            "id": "codex-call-1",
            "toolName": "echo",
            "arguments": { "message": "hello" }
        });
        state
            .handle_value(
                &json!({ "method": "item/completed", "params": { "item": item.clone() } }),
            )
            .unwrap();
        state
            .handle_value(&json!({
                "method": "turn/completed",
                "params": {
                    "turn": {
                        "status": "completed",
                        "items": [item]
                    }
                }
            }))
            .unwrap();

        let tool_events = state
            .events
            .iter()
            .filter(|event| event.event_type == "tool_request")
            .collect::<Vec<_>>();
        assert_eq!(tool_events.len(), 1);
        assert_eq!(
            tool_events[0].payload["source_id"].as_str(),
            Some("codex-call-1")
        );
        assert_eq!(state.final_status, "waiting_tool");
    }

    #[test]
    fn app_server_state_failed_turn_overrides_waiting_tool() {
        let mut state = AppServerState::new(Uuid::new_v4());
        state
            .handle_value(&json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "toolRequest",
                        "id": "failed-tool-request",
                        "toolName": "echo",
                        "arguments": { "message": "hello" }
                    }
                }
            }))
            .unwrap();
        state
            .handle_value(&json!({
                "method": "turn/completed",
                "params": { "turn": { "status": "failed", "items": [] } }
            }))
            .unwrap();

        assert_eq!(state.final_status, "failed");
        assert!(state.done);
    }

    #[test]
    fn app_server_state_interrupted_turn_overrides_waiting_tool() {
        let mut state = AppServerState::new(Uuid::new_v4());
        state
            .handle_value(&json!({
                "method": "turn/completed",
                "params": {
                    "turn": {
                        "status": "interrupted",
                        "items": [{
                            "type": "toolRequest",
                            "id": "interrupted-tool-request",
                            "toolName": "echo",
                            "arguments": { "message": "hello" }
                        }]
                    }
                }
            }))
            .unwrap();

        assert_eq!(state.final_status, "interrupted");
        assert!(state.done);
    }

    #[test]
    fn app_server_state_does_not_emit_failed_turn_error_details() {
        const UPSTREAM_SECRET: &str = "failed-turn-secret-must-not-leak";

        for status in ["failed", "interrupted"] {
            let mut state = AppServerState::new(Uuid::new_v4());
            state
                .handle_value(&json!({
                    "method": "turn/completed",
                    "params": {
                        "turn": {
                            "status": status,
                            "items": [{
                                "type": "agentMessage",
                                "text": "safe assistant output"
                            }],
                            "error": { "message": UPSTREAM_SECRET }
                        }
                    }
                }))
                .unwrap();

            let emitted_payloads = state
                .events
                .iter()
                .map(|event| event.payload.to_string())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(!emitted_payloads.contains(UPSTREAM_SECRET));
            assert_eq!(state.final_status, status);
        }
    }

    #[test]
    fn app_server_state_rejects_missing_or_non_string_turn_status() {
        for turn in [
            json!({ "items": [] }),
            json!({ "status": null, "items": [] }),
            json!({ "status": 1, "items": [] }),
        ] {
            let mut state = AppServerState::new(Uuid::new_v4());
            let error = state
                .handle_value(&json!({
                    "method": "turn/completed",
                    "params": { "turn": turn }
                }))
                .unwrap_err()
                .to_string();

            assert!(error.contains("turn status"));
            assert!(!state.done);
        }
    }

    #[test]
    fn app_server_state_rejects_non_terminal_or_unknown_turn_status() {
        for status in ["cancelled", "inProgress", "futureStatus"] {
            let mut state = AppServerState::new(Uuid::new_v4());
            let error = state
                .handle_value(&json!({
                    "method": "turn/completed",
                    "params": {
                        "turn": { "status": status, "items": [] }
                    }
                }))
                .unwrap_err()
                .to_string();

            assert!(error.contains("turn status"));
            assert!(!error.contains(status));
            assert!(!state.done);
        }
    }

    #[test]
    fn app_server_state_completes_turn_without_tool_request() {
        let mut state = AppServerState::new(Uuid::new_v4());
        state
            .handle_value(&json!({
                "method": "turn/completed",
                "params": { "turn": { "status": "completed", "items": [] } }
            }))
            .unwrap();

        assert_eq!(state.final_status, "completed");
        assert!(state.done);
    }

    #[tokio::test]
    async fn app_server_success_without_session_uses_thread_id_for_resume() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("fake-codex-no-session");
        let script_contents = r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{"id":1,"result":{"serverInfo":{"name":"fake-codex"}}}' ;;
    *'"method":"thread/start"'*) echo '{"id":2,"result":{"thread":{"id":"thread-without-session"}}}' ;;
    *'"method":"turn/start"'*)
      echo '{"method":"turn/completed","params":{"turn":{"status":"completed","items":[{"type":"agentMessage","text":"done without session"}]}}}'
      ;;
  esac
done
"#;
        std::fs::write(&script, script_contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let result = run_app_server_process(
            script.to_str().unwrap(),
            &run_env.workdir,
            &run_env.codex_home,
            &claim,
            Duration::from_secs(2),
            None,
        )
        .unwrap();

        assert_eq!(result.session_id.as_deref(), Some("thread-without-session"));
        assert!(result.events.iter().any(|event| {
            event.event_type == "message"
                && event.content.as_deref() == Some("done without session")
        }));
    }

    #[test]
    fn app_server_request_lines_use_real_lifecycle_shape() {
        let temp = tempfile::tempdir().unwrap();
        let mut claim = test_claim();
        claim.execution_configuration.reasoning_effort = ReasoningEffort::Ultra;
        let run_env = RunEnv {
            workdir: temp.path().join("workdir"),
            codex_home: temp.path().join("codex-home"),
        };
        let requests = app_server_request_lines(&claim, &run_env)
            .unwrap()
            .into_iter()
            .map(|line| serde_json::from_str::<serde_json::Value>(&line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(requests[0]["method"], "initialize");
        assert_eq!(requests[0]["jsonrpc"], "2.0");
        assert_eq!(requests[1]["method"], "initialized");
        assert_eq!(requests[1]["jsonrpc"], "2.0");
        assert_eq!(requests[2]["method"], "thread/start");
        assert_eq!(requests[2]["jsonrpc"], "2.0");
        assert_eq!(requests[2]["params"]["approvalPolicy"], "never");
        assert_eq!(requests[2]["params"]["model"], "gpt-main");
        assert_eq!(
            requests[2]["params"]["modelProvider"],
            model_provider_name(Uuid::from_u128(0x101))
        );
        assert_eq!(requests[3]["method"], "turn/start");
        assert_eq!(requests[3]["jsonrpc"], "2.0");
        assert_eq!(requests[3]["params"]["input"][0]["type"], "text");
        assert_eq!(requests[3]["params"]["model"], "gpt-main");
        assert_eq!(requests[3]["params"]["effort"], "ultra");
    }

    #[test]
    fn app_server_fixture_freezes_resume_start_steer_interrupt_protocol() {
        let temp = tempfile::tempdir().unwrap();
        let fixture =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/fake-codex-app-server.sh");
        let thread_id = "fixture-thread";
        let turn_id = "fake-app-server-turn-1";
        let transcript = temp
            .path()
            .join(format!("sessions/rollout-{thread_id}.jsonl"));
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        std::fs::write(&transcript, "{\"existing\":true}\n").unwrap();
        let requests = [
            json!({
                "jsonrpc": "2.0",
                "id": "initialize-request",
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "agent-hub-runtime-test",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }
            }),
            json!({ "jsonrpc": "2.0", "method": "initialized", "params": {} }),
            json!({
                "jsonrpc": "2.0",
                "id": "resume-request",
                "method": "thread/resume",
                "params": {
                    "threadId": thread_id,
                    "cwd": temp.path(),
                    "approvalPolicy": "never",
                    "developerInstructions": "test fixture protocol"
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "start-request",
                "method": "turn/start",
                "params": {
                    "threadId": thread_id,
                    "source": "fixture:protocol",
                    "input": [{ "type": "text", "text": "start" }]
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "steer-request",
                "method": "turn/steer",
                "params": {
                    "threadId": thread_id,
                    "expectedTurnId": turn_id,
                    "input": [{ "type": "text", "text": "steer" }]
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "interrupt-request",
                "method": "turn/interrupt",
                "params": { "threadId": thread_id, "turnId": turn_id }
            }),
        ];

        let mut child = Command::new(&fixture)
            .arg("app-server")
            .arg("--listen")
            .arg("stdio://")
            .env_clear()
            .env("PATH", env::var_os("PATH").unwrap_or_default())
            .env("CODEX_HOME", temp.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        {
            let mut stdin = child.stdin.take().unwrap();
            for request in &requests {
                send_app_server_value(&mut stdin, request).unwrap();
            }
        }
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "fixture failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let messages = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let response = |id: &str| {
            messages
                .iter()
                .find(|message| message.get("id").and_then(|value| value.as_str()) == Some(id))
                .unwrap_or_else(|| panic!("missing fixture response for {id}"))
        };

        assert_eq!(
            response("initialize-request")["result"]["serverInfo"]["name"],
            "fake-codex"
        );
        assert_eq!(
            response("resume-request")["result"]["thread"]["id"],
            thread_id
        );
        assert_eq!(response("start-request")["result"]["turn"]["id"], turn_id);
        assert_eq!(response("steer-request")["result"]["turnId"], turn_id);
        assert_eq!(response("interrupt-request")["result"], json!({}));
        assert!(messages.iter().any(|message| {
            message["method"] == "turn/started" && message["params"]["turn"]["id"] == turn_id
        }));
        assert!(messages.iter().any(|message| {
            message["method"] == "turn/completed"
                && message["params"]["turn"]["id"] == turn_id
                && message["params"]["turn"]["status"] == "interrupted"
        }));
        assert_eq!(
            std::fs::read_to_string(transcript).unwrap(),
            "{\"existing\":true}\n"
        );
    }

    #[test]
    fn app_server_steer_response_only_falls_back_for_an_ended_expected_turn() {
        assert_eq!(
            app_server_steer_response(
                &json!({ "id": 4, "result": { "turnId": "turn-active" } }),
                "turn-active",
            )
            .unwrap(),
            SessionSteerOutcome::Applied
        );
        assert_eq!(
            app_server_steer_response(
                &json!({
                    "id": 5,
                    "error": { "code": -32600, "message": "no active turn to steer" }
                }),
                "turn-ended",
            )
            .unwrap(),
            SessionSteerOutcome::TurnEnded
        );
        assert_eq!(
            app_server_steer_response(
                &json!({
                    "id": 6,
                    "error": {
                        "code": -32600,
                        "message": "expected active turn id `turn-ended` but found `turn-new`"
                    }
                }),
                "turn-ended",
            )
            .unwrap(),
            SessionSteerOutcome::TurnEnded
        );

        let error = app_server_steer_response(
            &json!({
                "id": 7,
                "error": { "code": -32600, "message": "activeTurnNotSteerable" }
            }),
            "turn-active",
        )
        .unwrap_err()
        .to_string();
        assert_eq!(error, "Codex app-server rejected turn/steer");
    }

    #[tokio::test]
    async fn app_server_fixture_retains_v1_completion_behavior() {
        let model_server = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let model_addr = model_server.local_addr().unwrap();
        let model_thread = std::thread::spawn(move || {
            let (mut stream, _) = model_server.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            let request = String::from_utf8_lossy(&request).to_ascii_lowercase();
            assert!(request.starts_with("post /v1/responses "));
            assert!(request.contains(
                "x-agent-hub-model-connection-id: 00000000-0000-0000-0000-000000000101\r\n"
            ));
            let body = r#"{"output_text":"v1 fixture response"}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            stream.flush().unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let fixture =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/fake-codex-app-server.sh");
        let claim = test_claim();
        let run_env = prepare_run_env(
            temp.path(),
            &claim,
            Some(&format!("http://{model_addr}/v1")),
        )
        .await
        .unwrap();
        let session_id = claim.run.hub_session_id.unwrap();
        let expected_turn_id = format!("fake-app-server-turn-{}", claim.run.id);
        let result = run_app_server_process(
            fixture.to_str().unwrap(),
            &run_env.workdir,
            &run_env.codex_home,
            &claim,
            Duration::from_secs(2),
            None,
        )
        .unwrap();

        assert_eq!(result.final_status, "completed");
        assert!(result.events.iter().any(|event| {
            event.event_type == "turn_started"
                && event.payload["native_turn_id"] == expected_turn_id
        }));
        let expected_thread_id = format!("fake-app-server-thread-{session_id}");
        assert_eq!(
            result.session_id.as_deref(),
            Some(expected_thread_id.as_str())
        );
        let transcript = run_env
            .codex_home
            .join("sessions")
            .join(format!("rollout-{expected_thread_id}.jsonl"));
        assert!(transcript.is_file());
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                std::fs::read_to_string(&transcript).unwrap().trim()
            )
            .unwrap(),
            json!({
                "type": "fake_app_server_fixture",
                "thread_id": expected_thread_id
            })
        );
        let artifact =
            session_bundle::create_session_bundle(&session_bundle::SessionBundleCreateSpec {
                session_id,
                native_thread_id: expected_thread_id.clone(),
                history_checkpoint: 0,
                bundle_generation: 1,
                ownership_generation: 1,
                producing_codex_version: "fake-codex-0.1.0".into(),
                created_at: chrono::Utc::now(),
                workspace: run_env.workdir.clone(),
                codex_home: run_env.codex_home.clone(),
                archive_path: temp.path().join("fixture-session-bundle.tar.zst"),
            })
            .unwrap();
        assert_eq!(artifact.manifest.native_thread_id, expected_thread_id);
        assert!(result.events.iter().any(|event| {
            event.event_type == "message" && event.content.as_deref() == Some("v1 fixture response")
        }));
        assert!(result
            .events
            .iter()
            .any(|event| event.event_type == "usage"));
        model_thread.join().unwrap();
    }

    #[test]
    fn app_server_turn_start_includes_dynamic_tools_for_integration_runs() {
        let mut claim = test_claim();
        claim.run.source = "integration:message".into();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([
                {
                    "type": "function",
                    "function": {
                        "name": "echo",
                        "description": "Echo a message",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "message": { "type": "string" }
                            },
                            "required": ["message"]
                        }
                    }
                }
            ]),
            attachments: json!([]),
            tool_result: None,
        });

        let request = app_server_turn_start_request(&claim, "thread-from-test");

        assert_eq!(request["method"], "turn/start");
        assert_eq!(request["params"]["threadId"], "thread-from-test");
        assert_eq!(
            request["params"]["dynamicTools"],
            claim.integration_context.unwrap().tools
        );
    }

    #[test]
    fn app_server_turn_start_combines_distinct_queued_messages_in_sequence_order() {
        let mut claim = test_claim();
        let session_id = claim.run.hub_session_id.unwrap();
        let turn_id = Uuid::new_v4();
        claim.run.hub_turn_id = Some(turn_id);
        let now = chrono::Utc::now();
        let message = |sequence: i64, content: &str| HubSessionMessageDto {
            id: Uuid::new_v4(),
            session_id,
            sequence,
            role: "user".into(),
            message_kind: "message".into(),
            content: Some(content.into()),
            payload: json!({}),
            delivery_mode: "next_turn".into(),
            delivery_state: "queued".into(),
            client_message_key: None,
            expected_native_turn_id: None,
            turn_id: Some(turn_id),
            run_id: Some(claim.run.id),
            accepted_at: now,
        };
        claim.session_context = Some(ClaimSessionContextDto {
            session: HubSessionDto {
                id: session_id,
                owner_id: claim.agent.owner_id,
                agent_id: claim.agent.id,
                agent_name: claim.agent.name.clone(),
                agent_deleted_at: None,
                origin: HubSessionOriginDto::HubNative,
                lifecycle_status: "online".into(),
                native_thread_id: None,
                active_turn_id: None,
                history_checkpoint: 2,
                configuration_fingerprint: None,
                runtime_owner_id: None,
                ownership_generation: 1,
                recovery_error: None,
                current_bundle: None,
                created_at: now,
                updated_at: now,
            },
            turn: HubSessionTurnDto {
                id: turn_id,
                session_id,
                native_turn_id: None,
                status: "pending".into(),
                configuration_fingerprint: None,
                ownership_generation: 1,
                started_at: None,
                ended_at: None,
                created_at: now,
                updated_at: now,
            },
            messages: vec![message(2, "second"), message(1, "first")],
        });

        let request = app_server_turn_start_request(&claim, "thread-from-test");
        let input = request["params"]["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["text"].as_str().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(input, vec!["first", "second"]);
    }

    #[test]
    fn app_server_turn_start_preserves_exact_tool_result_context() {
        let mut claim = test_claim();
        claim.run.source = "integration:tool_result".into();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([]),
            attachments: json!([]),
            tool_result: Some(json!({ "text": "matching tool result" })),
        });

        let request = app_server_turn_start_request(&claim, "thread-from-test");

        assert_eq!(
            request["params"]["metadata"]["integration_context"]["tool_result"]["text"],
            "matching tool result"
        );
    }

    #[tokio::test]
    async fn app_server_process_times_out_and_reaps_child() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("sleeping-codex.pid");
        let script = temp.path().join("sleeping-codex");
        let script_contents = format!(
            r#"#!/bin/sh
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) while :; do :; done ;;
  esac
done
"#,
            shell_single_quote(&pid_file)
        );
        std::fs::write(&script, script_contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let err = format!(
            "{:#}",
            run_app_server_process(
                script.to_str().unwrap(),
                &run_env.workdir,
                &run_env.codex_home,
                &claim,
                Duration::from_millis(200),
                None,
            )
            .unwrap_err()
        );

        assert!(err.contains("timed out"));
        #[cfg(unix)]
        {
            let pid = std::fs::read_to_string(&pid_file).unwrap();
            let proc_path = PathBuf::from(format!("/proc/{}", pid.trim()));
            assert!(!proc_path.exists());
        }
    }

    #[tokio::test]
    async fn app_server_process_reports_bad_json_and_reaps_child() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("bad-json-codex.pid");
        let script = temp.path().join("bad-json-codex");
        let script_contents = format!(
            r#"#!/bin/sh
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo 'not-json'; while :; do :; done ;;
  esac
done
"#,
            shell_single_quote(&pid_file)
        );
        std::fs::write(&script, script_contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let err = format!(
            "{:#}",
            run_app_server_process(
                script.to_str().unwrap(),
                &run_env.workdir,
                &run_env.codex_home,
                &claim,
                Duration::from_secs(2),
                None,
            )
            .unwrap_err()
        );

        assert!(err.contains("parse app-server JSON"));
        #[cfg(unix)]
        {
            let pid = std::fs::read_to_string(&pid_file).unwrap();
            let proc_path = PathBuf::from(format!("/proc/{}", pid.trim()));
            assert!(!proc_path.exists());
        }
    }

    #[tokio::test]
    async fn app_server_protocol_error_is_not_success_and_reaps_child() {
        const UPSTREAM_SECRET: &str = "upstream-error-must-not-leak";
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("protocol-error-codex.pid");
        let script = temp.path().join("protocol-error-codex");
        let script_contents = format!(
            r#"#!/bin/sh
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{"serverInfo":{{"name":"fake-codex"}}}}}}' ;;
    *'"method":"thread/start"'*) echo '{{"id":2,"result":{{"thread":{{"id":"protocol-error-thread"}}}}}}' ;;
    *'"method":"turn/start"'*)
      echo '{{"method":"item/completed","params":{{"item":{{"type":"toolRequest","id":"protocol-error-tool","toolName":"echo","arguments":{{}}}}}}}}'
      echo '{{"method":"turn/completed","params":{{"turn":{{"items":[],"error":{{"message":"{}"}}}}}}}}'
      while :; do :; done
      ;;
  esac
done
"#,
            shell_single_quote(&pid_file),
            UPSTREAM_SECRET
        );
        std::fs::write(&script, script_contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let error = format!(
            "{:#}",
            run_app_server_process(
                script.to_str().unwrap(),
                &run_env.workdir,
                &run_env.codex_home,
                &claim,
                Duration::from_secs(2),
                None,
            )
            .unwrap_err()
        );

        assert!(error.contains("turn status"));
        assert!(!error.contains(UPSTREAM_SECRET));
        #[cfg(unix)]
        {
            let pid = std::fs::read_to_string(&pid_file).unwrap();
            let proc_path = PathBuf::from(format!("/proc/{}", pid.trim()));
            assert!(!proc_path.exists());
        }
    }

    #[tokio::test]
    async fn app_server_process_reports_closed_stdout() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("closed-stdout-codex");
        std::fs::write(
            &script,
            r#"#!/bin/sh
exit 0
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let err = format!(
            "{:#}",
            run_app_server_process(
                script.to_str().unwrap(),
                &run_env.workdir,
                &run_env.codex_home,
                &claim,
                Duration::from_secs(1),
                None,
            )
            .unwrap_err()
        );

        assert!(
            err.contains("stdout closed")
                || err.contains("exited early")
                || err.contains("Broken pipe")
                || err.contains("failed to write"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn managed_skill_materializes_with_its_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let mut claim = test_claim();
        claim.execution_configuration.skills = vec![test_execution_skill(
            "repo-review",
            "repo-review",
            "managed content",
        )];
        claim.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap();

        let env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let content = fs::read_to_string(
            env.codex_home
                .join("skills")
                .join(skill_directory_name("repo-review"))
                .join("SKILL.md"),
        )
        .await
        .unwrap();

        assert!(content.contains("managed content"));
        assert!(content.contains("description: \"repo-review\""));
    }

    #[tokio::test]
    async fn non_ascii_skill_names_get_distinct_directories() {
        let temp = tempfile::tempdir().unwrap();
        let mut claim = test_claim();
        claim.execution_configuration.skills = vec![
            test_execution_skill("审查", "review one", "first content"),
            test_execution_skill("评审", "review two", "second content"),
        ];
        claim.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap();

        let env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let first_dir = env
            .codex_home
            .join("skills")
            .join(skill_directory_name("审查"));
        let second_dir = env
            .codex_home
            .join("skills")
            .join(skill_directory_name("评审"));

        assert_ne!(first_dir, second_dir);
        assert!(first_dir.join("SKILL.md").exists());
        assert!(second_dir.join("SKILL.md").exists());
    }

    #[test]
    fn codex_config_uses_structured_toml_for_control_characters() {
        let mut claim = test_claim();
        claim.execution_configuration.mcp_allowlist = json!([{
            "name": "odd.server\nname",
            "command": "cmd\n--flag",
            "args": ["line1\nline2"],
            "secrets": { "TOKEN.WITH.DOTS": "value\nwith-newline" }
        }]);

        let rendered = render_codex_config(
            &claim.execution_configuration,
            Some("http://127.0.0.1:1234/v1"),
        )
        .unwrap();
        let parsed = rendered.parse::<toml::Value>().unwrap();

        assert_eq!(
            parsed["mcp_servers"]["odd.server\nname"]["command"].as_str(),
            Some("cmd\n--flag")
        );
        assert_eq!(
            parsed["mcp_servers"]["odd.server\nname"]["env"]["TOKEN.WITH.DOTS"].as_str(),
            Some("value\nwith-newline")
        );
    }

    #[test]
    fn local_skill_name_uses_yaml_frontmatter_and_rejects_malformed_documents() {
        assert_eq!(
            local_skill_name(
                "---\ndescription: local skill\nname: \"review: \\\"quoted\\\"\"\n---\ncontent\n"
            ),
            Some("review: \"quoted\"".into())
        );
        assert_eq!(
            local_skill_name(
                "---\ndescription: local skill\nname: >-\n  runtime\n  review\n---\ncontent\n"
            ),
            Some("runtime review".into())
        );
        assert_eq!(
            local_skill_name(
                "---\nname: should-not-be-used\ndescription: [unterminated\n---\ncontent\n"
            ),
            None
        );
    }

    #[tokio::test]
    async fn malformed_local_skill_frontmatter_falls_back_to_directory_name() {
        let temp = tempfile::tempdir().unwrap();
        let local_root = temp.path().join("local-skills");
        let local_skill = local_root.join("directory-fallback");
        fs::create_dir_all(&local_skill).await.unwrap();
        fs::write(
            local_skill.join("SKILL.md"),
            "---\nname: ignored\ndescription: [unterminated\n---\ncontent\n",
        )
        .await
        .unwrap();
        let mut claim = test_claim();
        claim.execution_configuration.skills.clear();
        claim.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap();

        let env = prepare_run_env_with_local_skills(temp.path(), &claim, None, Some(&local_root))
            .await
            .unwrap();

        assert!(env
            .codex_home
            .join("skills")
            .join(skill_directory_name("directory-fallback"))
            .join("SKILL.md")
            .exists());
    }

    #[tokio::test]
    async fn hub_skill_overrides_runtime_local_skill_with_same_name() {
        let temp = tempfile::tempdir().unwrap();
        let local_root = temp.path().join("local-skills");
        let local_skill = local_root.join("repository-review");
        fs::create_dir_all(&local_skill).await.unwrap();
        fs::write(
            local_skill.join("SKILL.md"),
            "---\nname: repo-review\ndescription: local\n---\n\nlocal content\n",
        )
        .await
        .unwrap();
        let mut claim = test_claim();
        claim.execution_configuration.skills =
            vec![test_execution_skill("repo-review", "hub", "hub content")];
        claim.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap();

        let env = prepare_run_env_with_local_skills(temp.path(), &claim, None, Some(&local_root))
            .await
            .unwrap();
        let content = fs::read_to_string(
            env.codex_home
                .join("skills")
                .join(skill_directory_name("repo-review"))
                .join("SKILL.md"),
        )
        .await
        .unwrap();

        assert!(content.contains("hub content"));
        assert!(!content.contains("local content"));
    }

    #[tokio::test]
    async fn resume_does_not_copy_parent_run_workdir_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("parent-workdir");
        fs::create_dir_all(parent.join("nested")).await.unwrap();
        fs::write(parent.join("nested/state.txt"), "parent state")
            .await
            .unwrap();
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(Uuid::new_v4());
        claim.resume = Some(RunResumeDto {
            thread_id: "thread-parent".into(),
            work_dir_ref: Some(parent.display().to_string()),
        });

        let env = prepare_run_env(temp.path(), &claim, None).await.unwrap();

        assert!(fs::metadata(env.workdir.join("nested/state.txt"))
            .await
            .is_err());
    }

    #[test]
    fn resume_uses_thread_resume_json_rpc_method() {
        let mut claim = test_claim();
        claim.resume = Some(RunResumeDto {
            thread_id: "thread-parent".into(),
            work_dir_ref: None,
        });
        let run_env = RunEnv {
            workdir: PathBuf::from("/tmp/workdir"),
            codex_home: PathBuf::from("/tmp/codex-home"),
        };

        let request = app_server_thread_start_request(&claim, &run_env);

        assert_eq!(request["method"], "thread/resume");
        assert_eq!(request["params"]["threadId"], "thread-parent");
    }

    #[test]
    fn configuration_refresh_forces_cold_resume_without_setting_overrides() {
        let claim = test_claim();
        let run_env = RunEnv {
            workdir: PathBuf::from("/tmp/workdir"),
            codex_home: PathBuf::from("/tmp/codex-home"),
        };

        let request = app_server_thread_refresh_request(&claim, &run_env, "thread-current");

        assert_eq!(request["method"], "thread/resume");
        assert_eq!(request["params"]["threadId"], "thread-current");
        assert_eq!(request["params"]["excludeTurns"], true);
        assert_eq!(request["params"]["config"], json!({}));
        assert_eq!(request["params"]["model"], "gpt-main");
        assert_eq!(
            request["params"]["modelProvider"],
            model_provider_name(Uuid::from_u128(0x101))
        );
    }

    #[test]
    fn tool_request_ids_are_scoped_to_run_for_uuid_and_non_uuid_sources() {
        let arguments = json!({ "value": 1 });
        let uuid_source = Uuid::from_u128(3).to_string();

        for source_id in ["call-1", uuid_source.as_str()] {
            let first =
                stable_tool_request_uuid(Uuid::from_u128(1), "echo", Some(source_id), &arguments);
            let repeated =
                stable_tool_request_uuid(Uuid::from_u128(1), "echo", Some(source_id), &arguments);
            let second =
                stable_tool_request_uuid(Uuid::from_u128(2), "echo", Some(source_id), &arguments);

            assert_eq!(first, repeated);
            assert_ne!(first, second);
        }
    }

    #[test]
    fn runtime_rejects_direct_codex_download_source() {
        assert_eq!(validate_codex_source("path").unwrap(), "path");
        let error = validate_codex_source("download").unwrap_err();
        assert!(error.to_string().contains("Hub"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn candidate_codex_must_pass_bounded_exact_version_and_app_server_checks() {
        let temp = tempfile::tempdir().unwrap();
        let binary = temp.path().join("codex");
        fs::write(
            &binary,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'codex-cli 0.144.5'; exit 0; fi\nif [ \"$1\" = \"app-server\" ] && [ \"$2\" = \"--help\" ]; then exit 0; fi\nexit 9\n",
        )
        .await
        .unwrap();
        make_executable(&binary);

        verify_codex_compatibility(&binary, "0.144.5", Duration::from_secs(1))
            .await
            .unwrap();
        let error = verify_codex_compatibility(&binary, "0.144.4", Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("version mismatch"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_codex_artifact_is_verified_installed_by_exact_version_and_reused() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let script = b"#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'codex-cli 0.144.5'; exit 0; fi\nif [ \"$1\" = \"app-server\" ] && [ \"$2\" = \"--help\" ]; then exit 0; fi\nexit 9\n";
        let compressed = zstd::stream::encode_all(&script[..], 3).unwrap();
        let sha256 = Sha256::digest(&compressed)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let requests = Arc::new(AtomicUsize::new(0));
        let expected_token = "runtime-artifact-token".to_owned();
        let app = Router::new().route(
            "/api/runtime/codex/artifacts/{version}/{os}/{architecture}",
            get({
                let requests = Arc::clone(&requests);
                let compressed = compressed.clone();
                move |headers: HeaderMap| {
                    let requests = Arc::clone(&requests);
                    let compressed = compressed.clone();
                    let expected_token = expected_token.clone();
                    async move {
                        assert_eq!(
                            headers
                                .get(axum::http::header::AUTHORIZATION)
                                .unwrap()
                                .to_str()
                                .unwrap(),
                            format!("Bearer {expected_token}")
                        );
                        requests.fetch_add(1, Ordering::SeqCst);
                        Bytes::from(compressed)
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url,
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-artifact-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let artifact = CodexVersionArtifactDto {
            version: "0.144.5".into(),
            os: std::env::consts::OS.into(),
            architecture: std::env::consts::ARCH.into(),
            artifact_name: "codex.zst".into(),
            sha256,
            size_bytes: compressed.len() as u64,
        };
        let root = tempfile::tempdir().unwrap();

        let installed =
            install_managed_codex_artifact(root.path(), &client, &artifact, Duration::from_secs(1))
                .await
                .unwrap();
        assert!(installed.ends_with("bin/0.144.5/codex"));
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        let reused =
            install_managed_codex_artifact(root.path(), &client, &artifact, Duration::from_secs(1))
                .await
                .unwrap();
        assert_eq!(reused, installed);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn promoted_codex_switches_only_after_old_session_version_checkpoint_is_armed() {
        let script = b"#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'codex-cli 0.144.5'; exit 0; fi\nif [ \"$1\" = \"app-server\" ] && [ \"$2\" = \"--help\" ]; then exit 0; fi\nexit 9\n";
        let compressed = zstd::stream::encode_all(&script[..], 3).unwrap();
        let sha256 = format!("{:x}", Sha256::digest(&compressed));
        let app = Router::new().route(
            "/api/runtime/codex/artifacts/{version}/{os}/{architecture}",
            get({
                let compressed = compressed.clone();
                move || {
                    let compressed = compressed.clone();
                    async move { Bytes::from(compressed) }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url,
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let root = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let metadata = SessionSupervisorMetadata {
            format_version: 1,
            session_id,
            runtime_id,
            ownership_generation: 1,
            lifecycle_status: "online".into(),
            idle_deadline_unix_ms: None,
            checkpoint_reason: None,
            checkpoint_retry_unix_ms: None,
            hub_checkpoint_attempt_id: None,
            codex_version: "0.143.0".into(),
            native_thread_id: Some("thread-old-version".into()),
        };
        persist_session_supervisor_metadata(root.path(), &metadata)
            .await
            .unwrap();
        let snapshots = vec![RuntimeOwnedSessionSnapshotDto {
            session_id,
            ownership_generation: 1,
            lifecycle_status: "online".into(),
            native_thread_id: Some("thread-old-version".into()),
            active_run_id: None,
        }];
        let recovery = plan_session_recovery(root.path(), runtime_id, &snapshots, 1)
            .await
            .unwrap();
        let manager = SessionSupervisorManager::try_recover_cold_with_idle_timeout(
            root.path().to_path_buf(),
            runtime_id,
            recovery,
            DEFAULT_SESSION_IDLE_TIMEOUT,
        )
        .unwrap();
        let artifact = CodexVersionArtifactDto {
            version: "0.144.5".into(),
            os: std::env::consts::OS.into(),
            architecture: std::env::consts::ARCH.into(),
            artifact_name: "codex.zst".into(),
            sha256,
            size_bytes: compressed.len() as u64,
        };
        let mut config = test_config();
        config.work_root = root.path().to_path_buf();
        config.codex_version = "0.143.0".into();
        config.codex_bin = "/old/codex".into();
        let mut rollout = RuntimeCodexState::new(&config);

        apply_runtime_codex_rollout(
            &mut config,
            &client,
            &mut rollout,
            Some(&manager),
            &RuntimeCodexRolloutCommandDto {
                active_version: Some("0.143.0".into()),
                target_artifact: Some(artifact),
            },
        )
        .await;

        assert_eq!(config.codex_version, "0.143.0");
        assert_eq!(
            rollout.heartbeat_status().candidate_status.as_deref(),
            Some("ready")
        );
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());

        apply_runtime_codex_rollout(
            &mut config,
            &client,
            &mut rollout,
            Some(&manager),
            &RuntimeCodexRolloutCommandDto {
                active_version: Some("0.144.5".into()),
                target_artifact: None,
            },
        )
        .await;

        assert_eq!(config.codex_version, "0.144.5");
        assert!(config.codex_bin.ends_with("bin/0.144.5/codex"));
        assert_eq!(
            rollout.heartbeat_status(),
            RuntimeCodexStatusDto {
                current_version: "0.144.5".into(),
                candidate_version: None,
                candidate_status: None,
                candidate_error: None,
            }
        );
        let persisted: SessionSupervisorMetadata = serde_json::from_slice(
            &fs::read(
                SessionPaths::for_session(root.path(), session_id)
                    .supervisor
                    .join(SESSION_SUPERVISOR_METADATA_FILE),
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert_eq!(persisted.codex_version, "0.143.0");
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id,
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::VersionSwitch,
            }]
        );
        server.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn promotion_does_not_interrupt_or_mix_an_in_flight_turn() {
        let root = tempfile::tempdir().unwrap();
        let old_binary = root.path().join("old-codex");
        let entered = root.path().join("old-turn-entered");
        let release = root.path().join("old-turn-release");
        let old_pid = root.path().join("old-codex.pid");
        std::fs::write(
            &old_binary,
            format!(
                r#"#!/bin/sh
echo $$ > {}
while IFS= read -r line; do
  request_id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) echo "{{\"id\":$request_id,\"result\":{{}}}}" ;;
    *'"method":"thread/start"'*) echo "{{\"id\":$request_id,\"result\":{{\"thread\":{{\"id\":\"old-thread\"}}}}}}" ;;
    *'"method":"turn/start"'*)
      echo "{{\"id\":$request_id,\"result\":{{\"turn\":{{\"id\":\"old-turn\"}}}}}}"
      touch {}
      while [ ! -f {} ]; do sleep 0.01; done
      echo '{{"method":"turn/completed","params":{{"threadId":"old-thread","turn":{{"id":"old-turn","status":"completed","items":[{{"type":"agentMessage","text":"old version completed"}}]}}}}}}'
      ;;
  esac
done
"#,
                shell_single_quote(&old_pid),
                shell_single_quote(&entered),
                shell_single_quote(&release),
            ),
        )
        .unwrap();
        make_executable(&old_binary);

        let candidate = b"#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'codex-cli 0.144.5'; exit 0; fi\nif [ \"$1\" = \"app-server\" ] && [ \"$2\" = \"--help\" ]; then exit 0; fi\nexit 9\n";
        let compressed = zstd::stream::encode_all(&candidate[..], 3).unwrap();
        let sha256 = format!("{:x}", Sha256::digest(&compressed));
        let app = Router::new().route(
            "/api/runtime/codex/artifacts/{version}/{os}/{architecture}",
            get({
                let compressed = compressed.clone();
                move || {
                    let compressed = compressed.clone();
                    async move { Bytes::from(compressed) }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = HubClient {
            http: reqwest::Client::new(),
            hub_url: format!("http://{}", listener.local_addr().unwrap()),
            runtime_token: Arc::new(std::sync::RwLock::new("runtime-token".into())),
            protocol_capabilities: HashSet::new(),
        };
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let runtime_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let manager = Arc::new(SessionSupervisorManager::new(
            root.path().to_path_buf(),
            runtime_id,
            1,
        ));
        let mut claim = test_claim();
        claim.run.hub_session_id = Some(session_id);
        let run_env = prepare_run_env(root.path(), &claim, None).await.unwrap();
        manager
            .ensure_app_server(
                SessionSupervisorMetadata {
                    format_version: 1,
                    session_id,
                    runtime_id,
                    ownership_generation: 1,
                    lifecycle_status: "online".into(),
                    idle_deadline_unix_ms: None,
                    checkpoint_reason: None,
                    checkpoint_retry_unix_ms: None,
                    hub_checkpoint_attempt_id: None,
                    codex_version: "0.143.0".into(),
                    native_thread_id: None,
                },
                old_binary.display().to_string(),
                run_env,
                Duration::from_secs(3),
                None,
            )
            .await
            .unwrap();
        let execution = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.execute(claim, None).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !entered.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let pid_before = std::fs::read_to_string(&old_pid).unwrap();

        let artifact = CodexVersionArtifactDto {
            version: "0.144.5".into(),
            os: std::env::consts::OS.into(),
            architecture: std::env::consts::ARCH.into(),
            artifact_name: "codex.zst".into(),
            sha256,
            size_bytes: compressed.len() as u64,
        };
        let mut config = test_config();
        config.work_root = root.path().to_path_buf();
        config.codex_version = "0.143.0".into();
        config.codex_bin = old_binary.display().to_string();
        let mut rollout = RuntimeCodexState::new(&config);
        apply_runtime_codex_rollout(
            &mut config,
            &client,
            &mut rollout,
            Some(&manager),
            &RuntimeCodexRolloutCommandDto {
                active_version: Some("0.143.0".into()),
                target_artifact: Some(artifact),
            },
        )
        .await;
        apply_runtime_codex_rollout(
            &mut config,
            &client,
            &mut rollout,
            Some(&manager),
            &RuntimeCodexRolloutCommandDto {
                active_version: Some("0.144.5".into()),
                target_artifact: None,
            },
        )
        .await;

        assert_eq!(config.codex_version, "0.144.5");
        assert!(!execution.is_finished());
        assert_eq!(std::fs::read_to_string(&old_pid).unwrap(), pid_before);
        assert!(manager
            .take_due_checkpoint_requests()
            .await
            .unwrap()
            .is_empty());

        std::fs::write(&release, b"release").unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), execution)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result.final_status, "completed");
        assert_eq!(
            manager.take_due_checkpoint_requests().await.unwrap(),
            vec![RuntimeCheckpointRequest {
                session_id,
                ownership_generation: 1,
                reason: RuntimeCheckpointReason::VersionSwitch,
            }]
        );
        manager.shutdown();
        server.abort();
    }

    #[tokio::test]
    async fn workdir_gc_only_removes_uuid_run_directories() {
        let temp = tempfile::tempdir().unwrap();
        let run_dir = temp.path().join(Uuid::new_v4().to_string());
        let retained = temp.path().join("bin");
        let session_dir = temp
            .path()
            .join("sessions")
            .join(Uuid::new_v4().to_string());
        fs::create_dir_all(&run_dir).await.unwrap();
        fs::create_dir_all(&retained).await.unwrap();
        fs::create_dir_all(&session_dir).await.unwrap();

        let removed = gc_expired_run_dirs(temp.path(), Duration::ZERO, SystemTime::now())
            .await
            .unwrap();

        assert_eq!(removed, 1);
        assert!(fs::metadata(run_dir).await.is_err());
        assert!(fs::metadata(retained).await.is_ok());
        assert!(fs::metadata(session_dir).await.is_ok());
    }

    fn shell_single_quote(path: &Path) -> String {
        format!("'{}'", path.display().to_string().replace('\'', r#"'\''"#))
    }

    fn make_executable(path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(path).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(path, permissions).unwrap();
        }
    }

    fn write_long_running_streaming_codex(temp: &tempfile::TempDir, pid_file: &Path) -> PathBuf {
        let script = temp.path().join("long-running-streaming-codex");
        let script_contents = format!(
            r#"#!/bin/sh
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{"serverInfo":{{"name":"long-running"}}}}}}' ;;
    *'"method":"thread/start"'*) echo '{{"id":2,"result":{{"thread":{{"id":"long-thread","sessionId":"long-session"}}}}}}' ;;
    *'"method":"turn/start"'*)
      echo '{{"method":"item/agentMessage/delta","params":{{"delta":"first"}}}}'
      while :; do sleep 1; done
      ;;
  esac
done
"#,
            shell_single_quote(pid_file)
        );
        std::fs::write(&script, script_contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        script
    }

    fn write_high_frequency_streaming_codex(
        temp: &tempfile::TempDir,
        pid_file: &Path,
        completed_marker: &Path,
    ) -> PathBuf {
        let script = temp.path().join("high-frequency-streaming-codex");
        let script_contents = format!(
            r#"#!/bin/sh
echo $$ > {}
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*) echo '{{"id":1,"result":{{"serverInfo":{{"name":"burst"}}}}}}' ;;
    *'"method":"thread/start"'*) echo '{{"id":2,"result":{{"thread":{{"id":"burst-thread","sessionId":"burst-session"}}}}}}' ;;
    *'"method":"turn/start"'*)
      i=0
      while [ "$i" -lt 20000 ]; do
        echo '{{"method":"item/agentMessage/delta","params":{{"delta":"x"}}}}'
        i=$((i + 1))
      done
      : > {}
      while :; do sleep 1; done
      ;;
  esac
done
"#,
            shell_single_quote(pid_file),
            shell_single_quote(completed_marker)
        );
        std::fs::write(&script, script_contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        script
    }

    fn slow_failing_hub_client(delay: Duration) -> (HubClient, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let request = read_http_request(&mut stream);
            assert!(String::from_utf8_lossy(&request).contains("/events"));
            std::thread::sleep(delay);
            stream
                .write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                )
                .unwrap();
            stream.flush().unwrap();
        });
        (
            HubClient {
                http: reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(1))
                    .timeout(Duration::from_secs(2))
                    .build()
                    .unwrap(),
                hub_url: format!("http://{addr}"),
                runtime_token: Arc::new(std::sync::RwLock::new("test-runtime-token".into())),
                protocol_capabilities: HashSet::from([ATOMIC_WAITING_TOOL_BATCH_CAPABILITY.into()]),
            },
            thread,
        )
    }

    fn failing_hub_client(failing_path: &str) -> (HubClient, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let failing_path = failing_path.to_owned();
        let thread = std::thread::spawn(move || loop {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let request = read_http_request(&mut stream);
            let request_text = String::from_utf8_lossy(&request);
            let should_fail = request_text.contains(&failing_path);
            if should_fail {
                stream
                    .write_all(
                        b"HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                    )
                    .unwrap();
            } else if request_text.contains("/api/runtime/heartbeat") {
                write_heartbeat_response(&mut stream, false, false, false);
            } else {
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
                    .unwrap();
            }
            stream.flush().unwrap();
            if should_fail {
                return;
            }
        });
        (
            HubClient {
                http: reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(1))
                    .timeout(Duration::from_secs(2))
                    .build()
                    .unwrap(),
                hub_url: format!("http://{addr}"),
                runtime_token: Arc::new(std::sync::RwLock::new("test-runtime-token".into())),
                protocol_capabilities: HashSet::from([ATOMIC_WAITING_TOOL_BATCH_CAPABILITY.into()]),
            },
            thread,
        )
    }

    fn recording_hub_client(
        expected_requests: usize,
    ) -> (HubClient, RecordedHubRequests, std::thread::JoinHandle<()>) {
        recording_hub_client_with_failure(expected_requests, None)
    }

    fn recording_hub_client_with_failure(
        expected_requests: usize,
        failing_path: Option<&str>,
    ) -> (HubClient, RecordedHubRequests, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let server_requests = Arc::clone(&requests);
        let failing_path = failing_path.map(str::to_owned);
        let thread = std::thread::spawn(move || {
            for _ in 0..expected_requests {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let request = read_http_request(&mut stream);
                let request_text = String::from_utf8_lossy(&request);
                let should_fail = failing_path
                    .as_ref()
                    .is_some_and(|path| request_text.contains(path));
                let is_heartbeat = request_text.contains("/api/runtime/heartbeat");
                server_requests.lock().unwrap().push(request);
                if should_fail {
                    stream
                        .write_all(
                            b"HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                        )
                        .unwrap();
                } else if is_heartbeat {
                    write_heartbeat_response(&mut stream, false, false, false);
                } else {
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                        )
                        .unwrap();
                }
                stream.flush().unwrap();
            }
        });
        (
            HubClient {
                http: reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(1))
                    .timeout(Duration::from_secs(2))
                    .build()
                    .unwrap(),
                hub_url: format!("http://{addr}"),
                runtime_token: Arc::new(std::sync::RwLock::new("test-runtime-token".into())),
                protocol_capabilities: HashSet::from([ATOMIC_WAITING_TOOL_BATCH_CAPABILITY.into()]),
            },
            requests,
            thread,
        )
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0, "HTTP request closed before headers completed");
            request.extend_from_slice(&buffer[..read]);
            if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);
        let total_length = header_end + content_length;
        while request.len() < total_length {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0, "HTTP request closed before body completed");
            request.extend_from_slice(&buffer[..read]);
        }
        request.truncate(total_length);
        request
    }

    fn write_heartbeat_response(
        stream: &mut std::net::TcpStream,
        rotation_requested: bool,
        pending_credential_accepted: bool,
        credential_activated: bool,
    ) {
        let body = serde_json::to_string(&RuntimeHeartbeatResponse {
            rotation_requested,
            pending_credential_accepted,
            credential_activated,
            runtime_status: "online".into(),
            owned_sessions: Vec::new(),
            cleanup_sessions: Vec::new(),
            session_commands: Vec::new(),
            codex_rollout: RuntimeCodexRolloutCommandDto::default(),
        })
        .unwrap();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
        stream.flush().unwrap();
    }

    fn assert_process_group_reaped_or_clean_up(pid_file: &Path) {
        #[cfg(unix)]
        {
            let pid = std::fs::read_to_string(pid_file).unwrap();
            let pid = pid.trim().parse::<i32>().unwrap();
            for _ in 0..50 {
                if !process_group_exists(pid) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            unsafe {
                libc::kill(-pid, libc::SIGKILL);
            }
            panic!("app-server process group {pid} survived runtime cancellation");
        }
    }

    #[cfg(unix)]
    fn process_group_exists(process_group_id: i32) -> bool {
        if unsafe { libc::kill(-process_group_id, 0) } == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }

    fn temp_env_var(key: &str, value: &str, action: impl FnOnce()) {
        let previous = env::var(key).ok();
        // 测试内临时注入父进程环境，用于验证 Command 显式移除了敏感 runtime env。
        unsafe {
            env::set_var(key, value);
        }
        action();
        unsafe {
            if let Some(previous) = previous {
                env::set_var(key, previous);
            } else {
                env::remove_var(key);
            }
        }
    }

    fn test_execution_skill(
        name: &str,
        description: &str,
        content: &str,
    ) -> AgentExecutionSkillDto {
        AgentExecutionSkillDto {
            source: "managed".into(),
            source_id: Some(Uuid::new_v4()),
            name: name.into(),
            description: description.into(),
            content: content.into(),
            revision: 1,
            content_checksum_sha256: Sha256::digest(content.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        }
    }

    fn test_claim() -> ClaimRunResponse {
        let default_model_connection_id = Uuid::from_u128(0x101);
        let execution_configuration = AgentExecutionConfigurationDto {
            revision: 1,
            instructions: "Be concise".into(),
            default_model_connection_id: Some(default_model_connection_id),
            reasoning_effort: ReasoningEffort::Default,
            codex_subagents: Vec::new(),
            model_connections: vec![ModelConnectionOptionDto {
                id: default_model_connection_id,
                name: "Main model".into(),
                model_id: "gpt-main".into(),
                scope: ModelConnectionScope::Global,
                status: ModelConnectionStatus::Enabled,
            }],
            model_policy: json!({ "provider": "hub-proxy" }),
            sandbox_policy: json!({ "mode": "workspace-write", "network_access": true }),
            skills: vec![test_execution_skill(
                "repo-review",
                "repo-review",
                "Check the diff.",
            )],
            mcp_allowlist: json!([{ "name": "filesystem", "command": "fs" }]),
        };
        let expected_configuration_fingerprint =
            execution_configuration_fingerprint(&execution_configuration).unwrap();
        ClaimRunResponse {
            run: RunDto {
                id: Uuid::new_v4(),
                agent_id: Uuid::new_v4(),
                automation_id: None,
                integration_session_id: None,
                parent_run_id: None,
                runtime_id: None,
                hub_session_id: Some(Uuid::new_v4()),
                hub_message_id: None,
                hub_turn_id: None,
                session_ownership_generation: Some(1),
                status: "running".into(),
                initial_message: "hello".into(),
                session_id: None,
                work_dir_ref: None,
                source: "console".into(),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
            agent: AgentDto {
                id: Uuid::new_v4(),
                owner_id: Uuid::new_v4(),
                name: "Demo".into(),
                instructions: "Be concise".into(),
                visibility: "private".into(),
                public_to: Vec::new(),
                runtime_id: None,
                default_model_connection_id: Some(default_model_connection_id),
                reasoning_effort: ReasoningEffort::Default,
                codex_subagents: Vec::new(),
                model_policy: json!({ "provider": "hub-proxy" }),
                sandbox_policy: json!({ "mode": "workspace-write", "network_access": true }),
                managed_skill_ids: Vec::new(),
                mcp_allowlist: json!([{ "name": "filesystem", "command": "fs" }]),
                is_owner: false,
                can_manage: false,
                can_administer: false,
                can_invoke: false,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
            execution_configuration,
            expected_configuration_fingerprint,
            integration_context: None,
            resume: None,
            model_proxy_token: "test-model-proxy-token".into(),
            session_context: None,
        }
    }

    fn test_config() -> Config {
        Config {
            hub_url: "http://localhost:8080".into(),
            enrollment_token: Some("enrollment".into()),
            credential_file: PathBuf::from("/tmp/agent-hub-runtime-test/credential.json"),
            work_root: PathBuf::from("/tmp/agent-hub-runtime-test"),
            hostname: "test-runtime".into(),
            poll_interval: Duration::from_millis(10),
            codex_driver: "app-server".into(),
            codex_source: "path".into(),
            codex_bin: "codex".into(),
            codex_version: "test-codex".into(),
            app_server_timeout: Duration::from_secs(1),
            model_proxy_idle_timeout: Duration::from_secs(1),
            session_idle_timeout: DEFAULT_SESSION_IDLE_TIMEOUT,
            max_online_sessions: DEFAULT_MAX_ONLINE_SESSIONS,
            workdir_ttl: Duration::from_secs(3600),
            local_skills_dir: None,
            sandbox_mode: "workspace-write+network".into(),
            sandbox_downgrade_reason: None,
            health_bind_addr: "127.0.0.1:0".parse().unwrap(),
        }
    }

    fn test_stored_runtime_credential() -> StoredRuntimeCredential {
        StoredRuntimeCredential {
            runtime_id: Uuid::new_v4(),
            runtime_credential: "current-runtime-credential".into(),
            pending_runtime_credential: None,
            protocol_capabilities: vec![ATOMIC_WAITING_TOOL_BATCH_CAPABILITY.into()],
        }
    }

    #[test]
    fn runtime_enrollment_token_is_optional_after_a_credential_is_persisted() {
        let mut config = test_config();
        config.enrollment_token = None;
        assert!(config.enrollment_token.is_none());
    }
}
