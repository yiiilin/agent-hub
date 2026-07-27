use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    env, fs as stdfs,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
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

#[cfg(test)]
use std::io::Read;

mod pi_driver;
mod session_bundle;

const DEFAULT_ENGINE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_MODEL_PROXY_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(900);
const CHECKPOINT_RETRY_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_MAX_ONLINE_SESSIONS: usize = 4;
const ENGINE_EVENT_QUEUE_CAPACITY: usize = 64;
const SESSION_SUPERVISOR_METADATA_FILE: &str = "session.json";
const SESSION_CLEANUP_DIRECTORY: &str = "session-cleanups";
const SESSION_CLEANUP_STATE_FILE: &str = "state.json";

#[derive(Clone)]
struct Config {
    hub_url: String,
    enrollment_token: Option<String>,
    credential_file: PathBuf,
    work_root: PathBuf,
    hostname: String,
    poll_interval: Duration,
    engine_driver: String,
    engine_bin: String,
    engine_version: String,
    engine_timeout: Duration,
    model_proxy_idle_timeout: Duration,
    session_idle_timeout: Duration,
    max_online_sessions: usize,
    workdir_ttl: Duration,
    local_skills_dir: Option<PathBuf>,
    sandbox_mode: String,
    sandbox_downgrade_reason: Option<String>,
    health_bind_addr: SocketAddr,
}

#[derive(Debug)]
struct EngineRunResult {
    events: Vec<AppendRunEventRequest>,
    final_status: String,
    native_session_id: Option<String>,
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
        local_skills_dir: Option<&Path>,
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
            let local_skills_dir = local_skills_dir.map(Path::to_path_buf);
            tokio::spawn(async move {
                dispatcher
                    .run_session_queue(client, manager, session_id, local_skills_dir)
                    .await;
            });
        }
    }

    async fn run_session_queue(
        self: Arc<Self>,
        client: HubClient,
        manager: Arc<SessionSupervisorManager>,
        session_id: Uuid,
        local_skills_dir: Option<PathBuf>,
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
            match apply_runtime_session_command(&manager, &command, local_skills_dir.as_deref())
                .await
            {
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
struct EngineCancellation {
    cancelled: AtomicBool,
    #[cfg(unix)]
    process_group_id: AtomicI32,
}

impl EngineCancellation {
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
    resolve_engine_binary(&mut config).await?;
    gc_expired_run_dirs(&config.work_root, config.workdir_ttl, SystemTime::now()).await?;
    run_loop(config, health).await
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let engine_driver = validate_engine_driver(
            &env::var("RUNTIME_ENGINE_DRIVER").unwrap_or_else(|_| "fake".into()),
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
            engine_driver,
            engine_bin: env::var("ENGINE_BIN").unwrap_or_else(|_| "pi".into()),
            engine_version: env::var("RUNTIME_ENGINE_VERSION")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "unmanaged".into()),
            engine_timeout: Duration::from_secs(
                env::var("RUNTIME_ENGINE_TIMEOUT_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(DEFAULT_ENGINE_TIMEOUT.as_secs()),
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

fn validate_engine_driver(value: &str) -> anyhow::Result<String> {
    match value {
        "fake" | "pi" => Ok(value.to_owned()),
        _ => anyhow::bail!("RUNTIME_ENGINE_DRIVER must be 'fake' or 'pi'"),
    }
}

async fn resolve_engine_binary(config: &mut Config) -> anyhow::Result<()> {
    if config.engine_driver != "pi" {
        return Ok(());
    }
    config.engine_bin = locate_executable(&config.engine_bin)
        .with_context(|| format!("locate Execution Engine binary: {}", config.engine_bin))?
        .display()
        .to_string();
    Ok(())
}

fn locate_executable(value: &str) -> anyhow::Result<PathBuf> {
    let candidate = PathBuf::from(value);
    if candidate.components().count() > 1 {
        return executable_file(candidate);
    }
    let path = env::var_os("PATH").context("PATH is required to locate Execution Engine")?;
    for directory in env::split_paths(&path) {
        let candidate = directory.join(value);
        if let Ok(path) = executable_file(candidate) {
            return Ok(path);
        }
    }
    anyhow::bail!("Execution Engine executable was not found in PATH")
}

fn executable_file(path: PathBuf) -> anyhow::Result<PathBuf> {
    let metadata = stdfs::metadata(&path)?;
    if !metadata.is_file() {
        anyhow::bail!("Execution Engine path is not a file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            anyhow::bail!("Execution Engine path is not executable");
        }
    }
    Ok(stdfs::canonicalize(path)?)
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
        labels: vec!["local".into(), format!("driver:{}", config.engine_driver)],
        engine_version: if config.engine_driver == "pi" {
            config.engine_version.clone()
        } else {
            "fake-engine-0.1".into()
        },
        capabilities: json!({
            "driver": config.engine_driver,
            "platform": {
                "os": std::env::consts::OS,
                "architecture": std::env::consts::ARCH
            },
            "model_proxy": true,
            "mcp_allowlist": false,
            "subagents": false,
            "native_session_resume": true,
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
        let heartbeat =
            match send_runtime_heartbeat(config, client, stored, manager.as_deref()).await {
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
            command_dispatcher.enqueue(
                client,
                &session_manager,
                &heartbeat.session_commands,
                config.local_skills_dir.as_deref(),
            );
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
                producing_engine_version: config.engine_version.clone(),
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
    local_skills_dir: Option<&Path>,
) -> anyhow::Result<AppliedRuntimeSessionCommand> {
    match command.command.as_str() {
        "refresh_configuration" => {
            let configuration = command
                .execution_configuration
                .as_ref()
                .context("configuration refresh is missing its execution configuration")?;
            let revision = command
                .configuration_revision
                .context("configuration refresh is missing its revision")?;
            let fingerprint = command
                .fingerprint
                .as_deref()
                .context("configuration refresh is missing its fingerprint")?;
            anyhow::ensure!(
                configuration.revision == revision,
                "configuration refresh revision does not match its execution configuration"
            );
            anyhow::ensure!(
                configuration.model_bindings.is_empty(),
                "configuration refresh must not contain Run Model Bindings"
            );
            let runtime_fingerprint = execution_configuration_fingerprint(configuration)
                .context("validate refreshed Agent execution configuration")?;
            anyhow::ensure!(
                runtime_fingerprint == fingerprint,
                "configuration refresh fingerprint does not match its execution configuration"
            );
            match manager
                .refresh_execution_configuration(
                    command.session_id,
                    command.ownership_generation,
                    configuration,
                    fingerprint,
                    local_skills_dir,
                )
                .await
            {
                Ok(()) => Ok(AppliedRuntimeSessionCommand {
                    outcome: "applied",
                    native_error: None,
                }),
                Err(error) => Ok(AppliedRuntimeSessionCommand {
                    outcome: "failed",
                    native_error: Some(error),
                }),
            }
        }
        "steer" => {
            let message = command
                .message
                .as_ref()
                .context("steer command is missing its Hub message")?;
            anyhow::ensure!(
                message.id == command.command_id,
                "steer command id does not match its Hub message"
            );
            let native_session_id = command
                .native_session_id
                .as_deref()
                .context("steer command is missing its Native Session id")?;
            let native_turn_id = command
                .native_turn_id
                .clone()
                .context("steer command is missing its expected native Turn id")?;
            let outcome = manager
                .steer(
                    command.session_id,
                    command.ownership_generation,
                    native_session_id,
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
            let native_session_id = command
                .native_session_id
                .as_deref()
                .context("interrupt command is missing its Native Session id")?;
            let native_turn_id = command
                .native_turn_id
                .clone()
                .context("interrupt command is missing its native Turn id")?;
            let outcome = manager
                .interrupt(
                    command.session_id,
                    command.ownership_generation,
                    native_session_id,
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
    let result = if config.engine_driver == "pi" {
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
) -> anyhow::Result<RuntimeHeartbeatResponse> {
    let request = manager
        .map(SessionSupervisorManager::heartbeat_request)
        .unwrap_or_default();
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
                "x-agent-hub-producing-engine-version",
                &artifact.manifest.producing_engine_version,
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
                native_session_id: None,
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
        engine_state_root = %run_env.engine_state_root.display(),
        "claimed run"
    );

    let mut last_heartbeat = Instant::now();
    let (events, final_status) = fake_engine_events(&claim);
    let native_session_id = Some(format!("fake-session-{}", claim.run.id));
    finish_claimed_run(
        client,
        &claim,
        &run_env,
        events,
        final_status,
        native_session_id,
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
    native_session_id: Option<String>,
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
        native_session_id.as_deref(),
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
                    native_session_id,
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
                    native_session_id,
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
    engine_version: &str,
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
        engine_version: engine_version.to_owned(),
        native_session_id: claim
            .session_context
            .as_ref()
            .and_then(|context| context.session.native_session_id.clone()),
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
                && manifest.producing_engine_version == current.producing_engine_version,
            "Session Bundle manifest does not match Hub Bundle metadata"
        );
        anyhow::ensure!(
            context.session.native_session_id.as_deref() == Some(&manifest.native_session_id),
            "Session Bundle Native Session does not match Hub Session"
        );
        let metadata = session_supervisor_metadata_for_claim(
            configured_runtime_id(claim)?,
            claim,
            &config.engine_version,
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
        config.engine_driver == "pi",
        "persistent Session execution requires the Pi driver"
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
        session_supervisor_metadata_for_claim(manager.runtime_id, &claim, &config.engine_version)?;
    manager
        .ensure_pi(
            metadata,
            config.engine_bin.clone(),
            run_env.clone(),
            pi_driver::pi_tool_allowlist_for_claim(&claim)?,
            config.engine_timeout,
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
        engine_state_root = %run_env.engine_state_root.display(),
        "claimed persistent Session run"
    );

    let mut last_heartbeat = Instant::now();
    let result = execute_managed_pi_with_streaming(
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
        "native Pi Turn finished"
    );
    manager
        .update_native_session_id(
            session_id,
            claim
                .run
                .session_ownership_generation
                .context("claimed Run is missing its Session ownership generation")?,
            result.native_session_id.as_deref(),
        )
        .await?;
    finish_claimed_run(
        client,
        &claim,
        &run_env,
        result.events,
        result.final_status,
        result.native_session_id,
        &mut last_heartbeat,
    )
    .await
}

async fn execute_managed_pi_with_streaming(
    client: &HubClient,
    manager: Arc<SessionSupervisorManager>,
    claim: &ClaimRunResponse,
    last_heartbeat: &mut Instant,
    heartbeat_interval: Duration,
) -> anyhow::Result<EngineRunResult> {
    let (event_tx, mut event_rx) = engine_event_channel();
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
                let turn_started = event.event_type == "turn_started";
                persist_managed_native_session_from_event(
                    &manager,
                    session_id,
                    ownership_generation,
                    &event,
                ).await?;
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
                    if turn_started {
                        manager.acknowledge_model_proxy_turn(session_id, run_id)?;
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
                    let turn_started = event.event_type == "turn_started";
                    persist_managed_native_session_from_event(
                        &manager,
                        session_id,
                        ownership_generation,
                        &event,
                    ).await?;
                    if let Some(event) = defer_tool_request(event, &mut deferred_tool_requests) {
                        append_streamed_event(
                            client,
                            run_id,
                            ownership_generation,
                            event,
                            last_heartbeat,
                        ).await?;
                        if turn_started {
                            manager.acknowledge_model_proxy_turn(session_id, run_id)?;
                        }
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

async fn persist_managed_native_session_from_event(
    manager: &SessionSupervisorManager,
    session_id: Uuid,
    ownership_generation: i64,
    event: &AppendRunEventRequest,
) -> anyhow::Result<()> {
    if event.event_type != "turn_started" {
        return Ok(());
    }
    let native_session_id = event
        .payload
        .get("native_session_id")
        .and_then(serde_json::Value::as_str)
        .context("turn_started event is missing Native Session id")?;
    manager
        .update_native_session_id(session_id, ownership_generation, Some(native_session_id))
        .await
}

fn engine_event_channel() -> (
    tokio_mpsc::Sender<AppendRunEventRequest>,
    tokio_mpsc::Receiver<AppendRunEventRequest>,
) {
    tokio_mpsc::channel(ENGINE_EVENT_QUEUE_CAPACITY)
}

fn send_engine_event_with_backpressure(
    event_tx: &tokio_mpsc::Sender<AppendRunEventRequest>,
    mut event: AppendRunEventRequest,
    cancellation: &EngineCancellation,
) -> anyhow::Result<()> {
    loop {
        if cancellation.is_cancelled() {
            anyhow::bail!("stream Execution Engine event cancelled");
        }
        match event_tx.try_send(event) {
            Ok(()) => return Ok(()),
            Err(tokio_mpsc::error::TrySendError::Full(returned)) => {
                event = returned;
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(tokio_mpsc::error::TrySendError::Closed(_)) => {
                anyhow::bail!("stream Execution Engine event receiver closed");
            }
        }
    }
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
    native_session_id: Option<&str>,
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
        native_session_id: native_session_id
            .context("waiting tool run is missing a Native Session id")?
            .to_owned(),
        work_dir_ref: work_dir_ref.to_owned(),
        tool_requests,
    }))
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
            turn_acknowledged: false,
        };
        self.state.turn_acknowledged.notify_waiters();
    }

    fn acknowledge_turn(&self, run_id: Uuid) -> anyhow::Result<()> {
        let mut active_run = self.state.active_run.write().unwrap();
        anyhow::ensure!(
            active_run.run_id == run_id,
            "model proxy Run changed before Turn acknowledgement"
        );
        active_run.turn_acknowledged = true;
        drop(active_run);
        self.state.turn_acknowledged.notify_waiters();
        Ok(())
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
    turn_acknowledged: bool,
}

struct LocalModelProxyState {
    http: reqwest::Client,
    hub_url: String,
    active_run: std::sync::RwLock<LocalModelProxyRunAuth>,
    turn_acknowledged: Notify,
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
            turn_acknowledged: false,
        }),
        turn_acknowledged: Notify::new(),
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
    if headers.contains_key("x-agent-hub-model-connection-id") {
        return (
            AxumStatusCode::BAD_REQUEST,
            Json(json!({ "error": "Model Connection ID routing is not supported" })),
        )
            .into_response();
    }
    let binding_id = match headers
        .get("x-agent-hub-model-binding-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
    {
        Some(binding_id) => binding_id,
        None => {
            return (
                AxumStatusCode::BAD_REQUEST,
                Json(json!({ "error": "valid Run Model Binding ID is required" })),
            )
                .into_response();
        }
    };
    let requested_run_id = state.active_run.read().unwrap().run_id;
    let active_run = loop {
        let acknowledged = state.turn_acknowledged.notified();
        let active_run = state.active_run.read().unwrap().clone();
        if active_run.run_id != requested_run_id {
            return (
                AxumStatusCode::CONFLICT,
                Json(json!({ "error": "model proxy Run changed before Turn acknowledgement" })),
            )
                .into_response();
        }
        if active_run.turn_acknowledged {
            break active_run;
        }
        acknowledged.await;
    };
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
        .header("x-agent-hub-model-binding-id", binding_id.to_string())
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
            || name == "x-agent-hub-model-binding-id"
            || name == "x-agent-hub-model-connection-id"
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionSteerOutcome {
    Applied,
    TurnEnded,
}

struct PendingSteerResponse {
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
        response: oneshot::Sender<anyhow::Result<EngineRunResult>>,
    },
    Steer {
        expected_turn_id: String,
        input: Vec<String>,
        response: oneshot::Sender<anyhow::Result<SessionSteerOutcome>>,
    },
    Interrupt {
        expected_turn_id: String,
        response: oneshot::Sender<anyhow::Result<SessionInterruptOutcome>>,
    },
    RefreshConfiguration {
        response: oneshot::Sender<anyhow::Result<()>>,
    },
    Stop,
}

struct SessionSupervisor {
    session_id: Uuid,
    ownership_generation: i64,
    command_tx: mpsc::Sender<SessionSupervisorCommand>,
    cancellation: Arc<EngineCancellation>,
    actor: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    terminal_error: Arc<std::sync::Mutex<Option<String>>>,
    stopped: AtomicBool,
}

enum PersistentSessionProcess {
    Pi(pi_driver::PersistentPiRpcProcess),
}

impl PersistentSessionProcess {
    fn execute_controlled(
        &mut self,
        claim: &ClaimRunResponse,
        event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
        command_rx: &mpsc::Receiver<SessionSupervisorCommand>,
        deferred_commands: &mut VecDeque<SessionSupervisorCommand>,
    ) -> anyhow::Result<EngineRunResult> {
        match self {
            Self::Pi(process) => {
                process.execute_controlled(claim, event_tx, command_rx, deferred_commands)
            }
        }
    }

    fn ensure_running(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Pi(process) => process.ensure_running(),
        }
    }
}

enum SessionProcessLaunch {
    Pi {
        binary: String,
        run_env: RunEnv,
        saved_session: Option<PathBuf>,
        tools: Vec<String>,
        timeout: Duration,
    },
}

impl SessionProcessLaunch {
    fn timeout(&self) -> Duration {
        match self {
            Self::Pi { timeout, .. } => *timeout,
        }
    }

    fn start(
        self,
        cancellation: Arc<EngineCancellation>,
    ) -> anyhow::Result<PersistentSessionProcess> {
        match self {
            Self::Pi {
                binary,
                run_env,
                saved_session,
                tools,
                timeout,
            } => pi_driver::PersistentPiRpcProcess::start(
                &binary,
                &run_env,
                saved_session.as_deref(),
                &tools,
                timeout,
                cancellation,
            )
            .map(PersistentSessionProcess::Pi),
        }
    }
}

impl SessionSupervisor {
    async fn start_pi(
        session_id: Uuid,
        ownership_generation: i64,
        pi_bin: String,
        run_env: RunEnv,
        saved_session: Option<PathBuf>,
        tools: Vec<String>,
        timeout: Duration,
    ) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(
            ownership_generation > 0,
            "ownership generation must be positive"
        );
        tokio::task::spawn_blocking(move || {
            Self::start_process_blocking(
                session_id,
                ownership_generation,
                SessionProcessLaunch::Pi {
                    binary: pi_bin,
                    run_env,
                    saved_session,
                    tools,
                    timeout,
                },
            )
        })
        .await?
    }

    fn start_process_blocking(
        session_id: Uuid,
        ownership_generation: i64,
        launch: SessionProcessLaunch,
    ) -> anyhow::Result<Arc<Self>> {
        let startup_timeout = launch.timeout();
        let (command_tx, command_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let cancellation = Arc::new(EngineCancellation::default());
        let actor_cancellation = Arc::clone(&cancellation);
        let terminal_error = Arc::new(std::sync::Mutex::new(None));
        let actor_terminal_error = Arc::clone(&terminal_error);
        let actor = std::thread::spawn(move || {
            let mut process = match launch.start(actor_cancellation) {
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
                    Ok(SessionSupervisorCommand::RefreshConfiguration { response }) => {
                        let _ = response.send(Ok(()));
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
        match ready_rx.recv_timeout(startup_timeout + Duration::from_secs(1)) {
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
    ) -> anyhow::Result<EngineRunResult> {
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
        _client_user_message_id: Uuid,
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

    async fn wait_for_configuration_refresh(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.stopped.load(Ordering::Acquire),
            "Session supervisor is stopped"
        );
        let (response, result) = oneshot::channel();
        self.command_tx
            .send(SessionSupervisorCommand::RefreshConfiguration { response })
            .map_err(|_| anyhow::anyhow!("Session supervisor actor is not running"))?;
        result.await.map_err(|_| {
            anyhow::anyhow!("Session supervisor actor stopped before configuration refresh")
        })?
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
        tool_allowlist: BTreeSet<String>,
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
}

fn checkpoint_reason_priority(reason: RuntimeCheckpointReason) -> u8 {
    match reason {
        RuntimeCheckpointReason::Idle => 0,
        RuntimeCheckpointReason::Drain => 1,
    }
}

impl RuntimeCheckpointReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
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
    producing_engine_version: String,
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
        let fallback_producing_engine_version = self.producing_engine_version.clone();
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
            let producing_engine_version = if metadata.engine_version.trim().is_empty() {
                fallback_producing_engine_version
            } else {
                metadata.engine_version.clone()
            };
            let native_session_id = metadata
                .native_session_id
                .context("Session checkpoint has no Native Session id")?;
            let archive_path = paths.staging.join(format!(
                "bundle-{}-{}.tar.zst",
                bundle_generation, checkpoint_attempt_id
            ));
            session_bundle::create_session_bundle(&session_bundle::SessionBundleCreateSpec {
                session_id,
                native_session_id,
                history_checkpoint,
                bundle_generation,
                ownership_generation,
                producing_engine_version,
                created_at: chrono::Utc::now(),
                workspace: paths.workspace,
                engine_state_root: paths.engine_state,
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
                        ..
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
            &paths.engine_state,
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
        let native_session_id = claim
            .session_context
            .as_ref()
            .and_then(|context| context.session.native_session_id.clone());
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
            engine_version: String::new(),
            native_session_id: native_session_id.clone(),
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
                    native_session_id,
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

    async fn ensure_pi(
        &self,
        metadata: SessionSupervisorMetadata,
        pi_bin: String,
        run_env: RunEnv,
        tools: Vec<String>,
        timeout: Duration,
        model_proxy: Option<Arc<LocalModelProxy>>,
    ) -> anyhow::Result<Arc<SessionSupervisor>> {
        let session_id = metadata.session_id;
        let ownership_generation = metadata.ownership_generation;
        let native_session_id = metadata.native_session_id.clone();
        let launch_tools = tools.clone();
        self.ensure_session_supervisor(metadata, model_proxy, tools, move || async move {
            let saved_session = native_session_id
                .as_deref()
                .map(|session_id| {
                    pi_driver::discover_session_file(&run_env.engine_state_root, session_id)
                })
                .transpose()?;
            SessionSupervisor::start_pi(
                session_id,
                ownership_generation,
                pi_bin,
                run_env,
                saved_session,
                launch_tools,
                timeout,
            )
            .await
        })
        .await
    }

    async fn ensure_session_supervisor<F, Fut>(
        &self,
        metadata: SessionSupervisorMetadata,
        model_proxy: Option<Arc<LocalModelProxy>>,
        tools: Vec<String>,
        start: F,
    ) -> anyhow::Result<Arc<SessionSupervisor>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Arc<SessionSupervisor>>>,
    {
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
        let tool_allowlist = tools.into_iter().collect::<BTreeSet<_>>();
        let supervisor_to_stop = {
            let mut records = self.records.lock().unwrap();
            if let Some(record) = records.get_mut(&metadata.session_id) {
                anyhow::ensure!(
                    record.snapshot.ownership_generation == metadata.ownership_generation,
                    "Session manager has a different ownership generation"
                );
                match &record.status {
                    ManagedSessionStatus::Ready {
                        metadata: current_metadata,
                        supervisor,
                        busy,
                        tool_allowlist: current_tool_allowlist,
                    } => {
                        if *current_tool_allowlist == tool_allowlist {
                            return Ok(Arc::clone(supervisor));
                        }
                        anyhow::ensure!(
                            !*busy,
                            "Pi tool policy cannot change during an active Turn"
                        );
                        if let Some(current_native_session_id) =
                            current_metadata.native_session_id.as_ref()
                        {
                            anyhow::ensure!(
                                metadata.native_session_id.as_ref()
                                    == Some(current_native_session_id),
                                "Pi tool policy restart Native Session is missing or does not match"
                            );
                        }
                        if let Some(model_proxy) = model_proxy.as_ref() {
                            record.model_proxy = Some(Arc::clone(model_proxy));
                        }
                        let supervisor = Arc::clone(supervisor);
                        record.status = ManagedSessionStatus::Starting;
                        Some(supervisor)
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
                        None
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
                let snapshot = RuntimeOwnedSessionSnapshotDto {
                    session_id: metadata.session_id,
                    ownership_generation: metadata.ownership_generation,
                    lifecycle_status: metadata.lifecycle_status.clone(),
                    native_session_id: metadata.native_session_id.clone(),
                    active_run_id: None,
                };
                records.insert(
                    metadata.session_id,
                    ManagedSessionRecord {
                        snapshot,
                        status: ManagedSessionStatus::Starting,
                        reserved_run_id: None,
                        model_proxy: model_proxy.as_ref().map(Arc::clone),
                    },
                );
                None
            }
        };
        if let Some(supervisor) = supervisor_to_stop {
            supervisor.shutdown();
        }

        if let Err(error) = persist_session_supervisor_metadata(&self.work_root, &metadata).await {
            self.mark_blocked(metadata.session_id, error.to_string());
            return Err(error);
        }
        let supervisor = match start().await {
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
            tool_allowlist,
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
                        &record.paths.engine_state,
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
    ) -> anyhow::Result<EngineRunResult> {
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

    async fn update_native_session_id(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
        native_session_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let Some(native_session_id) = native_session_id else {
            return Ok(());
        };
        let metadata = {
            let records = self.records.lock().unwrap();
            let record = records
                .get(&session_id)
                .context("Session disappeared before Native Session persistence")?;
            anyhow::ensure!(
                record.snapshot.ownership_generation == ownership_generation,
                "Session ownership changed before Native Session persistence"
            );
            let mut metadata = match &record.status {
                ManagedSessionStatus::Ready { metadata, .. }
                    if metadata.native_session_id.as_deref() == Some(native_session_id) =>
                {
                    return Ok(())
                }
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
            metadata.native_session_id = Some(native_session_id.to_owned());
            metadata
        };
        persist_session_supervisor_metadata(&self.work_root, &metadata).await?;
        let mut records = self.records.lock().unwrap();
        let record = records
            .get_mut(&session_id)
            .context("Session disappeared after Native Session persistence")?;
        anyhow::ensure!(
            record.snapshot.ownership_generation == ownership_generation,
            "Session ownership changed after Native Session persistence"
        );
        let ManagedSessionStatus::Ready {
            metadata: current, ..
        } = &mut record.status
        else {
            anyhow::bail!("Session stopped while Native Session metadata was persisted");
        };
        *current = metadata.clone();
        record.snapshot.native_session_id = metadata.native_session_id;
        Ok(())
    }

    async fn steer(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
        native_session_id: &str,
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
                    ..
                } => {
                    anyhow::ensure!(
                        metadata.native_session_id.as_deref() == Some(native_session_id),
                        "Steering Message Native Session does not match Session metadata"
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

    async fn refresh_execution_configuration(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
        configuration: &AgentExecutionConfigurationDto,
        configuration_fingerprint: &str,
        local_skills_dir: Option<&Path>,
    ) -> anyhow::Result<()> {
        let supervisor = {
            let records = self.records.lock().unwrap();
            let record = records
                .get(&session_id)
                .context("configuration refresh Session is not managed by this Runtime")?;
            anyhow::ensure!(
                record.snapshot.ownership_generation == ownership_generation,
                "configuration refresh ownership generation is stale"
            );
            anyhow::ensure!(
                record.reserved_run_id.is_none(),
                "configuration refresh cannot overlap a reserved Run"
            );
            match &record.status {
                ManagedSessionStatus::Ready { supervisor, .. } => Some(Arc::clone(supervisor)),
                ManagedSessionStatus::Cold { .. } => None,
                ManagedSessionStatus::Starting => {
                    anyhow::bail!("Session supervisor is still starting")
                }
                ManagedSessionStatus::Blocked { reason, .. } => {
                    anyhow::bail!("Session supervisor is blocked: {reason}")
                }
            }
        };
        if let Some(supervisor) = supervisor {
            supervisor.wait_for_configuration_refresh().await?;
        }
        {
            let records = self.records.lock().unwrap();
            let record = records
                .get(&session_id)
                .context("configuration refresh Session disappeared")?;
            anyhow::ensure!(
                record.snapshot.ownership_generation == ownership_generation,
                "configuration refresh ownership changed before materialization"
            );
            anyhow::ensure!(
                record.reserved_run_id.is_none(),
                "configuration refresh overlapped a newly reserved Run"
            );
            match &record.status {
                ManagedSessionStatus::Ready { .. } | ManagedSessionStatus::Cold { .. } => {}
                ManagedSessionStatus::Starting => {
                    anyhow::bail!("Session supervisor started during configuration refresh")
                }
                ManagedSessionStatus::Blocked { reason, .. } => {
                    anyhow::bail!("Session supervisor is blocked: {reason}")
                }
            }
        }
        let paths = SessionPaths::for_session(&self.work_root, session_id);
        synchronize_pi_execution_configuration(
            &paths,
            configuration,
            configuration_fingerprint,
            PiModelConfigurationMaterialization::PreserveExisting,
            local_skills_dir,
        )
        .await
    }

    async fn interrupt(
        &self,
        session_id: Uuid,
        ownership_generation: i64,
        native_session_id: &str,
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
                    ..
                } => {
                    anyhow::ensure!(
                        metadata.native_session_id.as_deref() == Some(native_session_id),
                        "Interrupt Native Session does not match Session metadata"
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

    fn acknowledge_model_proxy_turn(&self, session_id: Uuid, run_id: Uuid) -> anyhow::Result<()> {
        self.model_proxy(session_id)
            .context("Session model proxy is unavailable")?
            .acknowledge_turn(run_id)
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
                let native_session_mismatch = match (&snapshot.native_session_id, &record.status) {
                    (
                        Some(hub_native_session_id),
                        ManagedSessionStatus::Cold { metadata }
                        | ManagedSessionStatus::Ready { metadata, .. },
                    ) => metadata.native_session_id.as_ref() != Some(hub_native_session_id),
                    _ => false,
                };
                if record.snapshot.ownership_generation != snapshot.ownership_generation
                    || native_session_mismatch
                {
                    if let ManagedSessionStatus::Ready { supervisor, .. } = &record.status {
                        supervisors_to_stop.push(Arc::clone(supervisor));
                    }
                    if let Some(proxy) = record.model_proxy.take() {
                        proxies_to_drop.push(proxy);
                    }
                    record.reserved_run_id = None;
                    record.status = ManagedSessionStatus::Blocked {
                        reason: if native_session_mismatch {
                            "Hub Native Session does not match local Session metadata".into()
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
                        ..
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

#[derive(Clone)]
struct RunEnv {
    workdir: PathBuf,
    engine_state_root: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SessionPaths {
    root: PathBuf,
    workspace: PathBuf,
    engine_state: PathBuf,
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
            engine_state: root.join("engine-state"),
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
    engine_version: String,
    native_session_id: Option<String>,
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
    stdfs::create_dir_all(&paths.engine_state)?;
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
                    && snapshot
                        .native_session_id
                        .as_ref()
                        .is_none_or(|native_session_id| {
                            metadata.native_session_id.as_ref() == Some(native_session_id)
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
    fs::create_dir_all(&paths.engine_state).await?;
    fs::create_dir_all(&paths.supervisor).await?;
    fs::create_dir_all(&paths.staging).await?;
    let runtime_fingerprint = execution_configuration_fingerprint(&claim.execution_configuration)
        .context("validate claimed Agent execution configuration")?;
    anyhow::ensure!(
        runtime_fingerprint == claim.expected_configuration_fingerprint,
        "Hub and Runtime execution configuration fingerprints differ"
    );
    synchronize_pi_execution_configuration(
        &paths,
        &claim.execution_configuration,
        &runtime_fingerprint,
        PiModelConfigurationMaterialization::RunBindings { model_base_url },
        local_skills_dir,
    )
    .await?;
    let run_env = RunEnv {
        workdir: paths.workspace,
        engine_state_root: paths.engine_state,
    };
    pi_driver::materialize_integration_tools(&run_env, claim.integration_context.as_ref())?;
    Ok(run_env)
}

const PI_MATERIALIZATION_MARKER_FILE: &str = ".agent-hub-materialization.json";
const PI_AGENT_DIRECTORY: &str = ".pi/agent";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PiExecutionMaterializationMarker {
    format_version: u32,
    configuration_fingerprint: String,
    materialization_sha256: String,
    owned_skill_directories: Vec<String>,
}

#[derive(Clone, Copy)]
enum PiModelConfigurationMaterialization<'a> {
    RunBindings { model_base_url: Option<&'a str> },
    PreserveExisting,
}

fn pi_agent_directory(pi_home: &Path) -> PathBuf {
    pi_home.join(PI_AGENT_DIRECTORY)
}

async fn synchronize_pi_execution_configuration(
    paths: &SessionPaths,
    configuration: &AgentExecutionConfigurationDto,
    configuration_fingerprint: &str,
    model_configuration: PiModelConfigurationMaterialization<'_>,
    local_skills_dir: Option<&Path>,
) -> anyhow::Result<()> {
    let agent_dir = pi_agent_directory(&paths.engine_state);
    let stage = paths
        .staging
        .join(format!("pi-execution-config-{}", Uuid::new_v4()));
    let stage_agent_dir = stage.join("agent");
    let result: anyhow::Result<()> = async {
        fs::create_dir_all(stage_agent_dir.join("skills")).await?;
        let instructions = format!("{}\n", configuration.instructions.trim_end());
        write_private_file(&stage_agent_dir.join("AGENTS.md"), instructions.as_bytes())
            .context("stage Pi Agent guidance")?;

        match model_configuration {
            PiModelConfigurationMaterialization::RunBindings { model_base_url } => {
                let models_json = render_pi_models_json(configuration, model_base_url)?;
                write_private_file(&stage_agent_dir.join("models.json"), models_json.as_bytes())
                    .context("stage per-Session Pi models config")?;
            }
            PiModelConfigurationMaterialization::PreserveExisting => {
                let _ = validated_pi_execution_materialization(&agent_dir)?;
                let models_json = stdfs::read_to_string(agent_dir.join("models.json"))
                    .context("read current per-Session Pi models config")?;
                let parsed = serde_json::from_str::<Value>(&models_json)
                    .context("parse current per-Session Pi models config")?;
                anyhow::ensure!(
                    parsed.get("providers").and_then(Value::as_object).is_some(),
                    "current per-Session Pi models config has no providers object"
                );
                write_private_file(&stage_agent_dir.join("models.json"), models_json.as_bytes())
                    .context("preserve per-Session Pi models config")?;
            }
        }

        write_private_file(
            &stage_agent_dir.join("skills-manifest.json"),
            &serde_json::to_vec_pretty(&configuration.skills)?,
        )
        .context("stage Pi Skills manifest")?;
        if let Some(local_skills_dir) = local_skills_dir {
            materialize_local_skills(&stage_agent_dir, local_skills_dir).await?;
        }
        let skills = serde_json::to_value(&configuration.skills)?;
        // Hub Skills are applied last so an inline/managed Skill overrides runtime-local content.
        materialize_skills(&stage_agent_dir, &skills).await?;
        let owned_skill_directories =
            skill_directory_entries(&stage_agent_dir.join("skills")).await?;
        let materialization_sha256 =
            pi_execution_materialization_sha256(&stage_agent_dir, &owned_skill_directories)?;
        let marker = PiExecutionMaterializationMarker {
            format_version: 1,
            configuration_fingerprint: configuration_fingerprint.to_owned(),
            materialization_sha256,
            owned_skill_directories,
        };
        write_private_file(
            &stage_agent_dir.join(PI_MATERIALIZATION_MARKER_FILE),
            &serde_json::to_vec_pretty(&marker)?,
        )
        .context("stage Pi execution configuration marker")?;

        if pi_materialization_is_current(&agent_dir, &marker) {
            return Ok(());
        }
        commit_pi_execution_materialization(&agent_dir, &stage_agent_dir, &marker).await
    }
    .await;
    if let Err(cleanup_error) = fs::remove_dir_all(&stage).await {
        if cleanup_error.kind() != std::io::ErrorKind::NotFound {
            if result.is_ok() {
                return Err(cleanup_error)
                    .context("remove Pi execution configuration staging directory");
            }
            warn!(
                path = %stage.display(),
                error = %cleanup_error,
                "failed to clean Pi execution configuration staging directory"
            );
        }
    }
    result
}

fn render_pi_models_json(
    configuration: &AgentExecutionConfigurationDto,
    model_base_url: Option<&str>,
) -> anyhow::Result<String> {
    let binding = model_binding(configuration, "main")?;
    let provider_name = pi_model_provider_name(binding.id);
    let base_url = model_base_url.unwrap_or("http://127.0.0.1:0/v1");
    let mut model = serde_json::Map::new();
    model.insert("id".into(), json!(binding.model_id));
    model.insert("name".into(), json!(binding.model_id));
    model.insert("input".into(), json!(["text"]));
    model.insert(
        "contextWindow".into(),
        json!(binding
            .model_settings
            .context_window_tokens
            .unwrap_or(128_000)),
    );
    model.insert(
        "maxTokens".into(),
        json!(pi_model_max_output_tokens(&binding.model_settings)),
    );
    if let Some(thinking_level) = pi_thinking_level(binding.model_settings.reasoning_effort) {
        model.insert("reasoning".into(), Value::Bool(true));
        model.insert(
            "thinkingLevelMap".into(),
            pi_thinking_level_map(binding.model_settings.reasoning_effort, thinking_level),
        );
    } else {
        model.insert("reasoning".into(), Value::Bool(false));
    }

    serde_json::to_string_pretty(&json!({
        "providers": {
            provider_name: {
                "baseUrl": base_url,
                "api": "openai-responses",
                // Pi needs an auth presence to expose a custom model. This is a
                // non-secret placeholder; the loopback proxy strips it.
                "apiKey": "agent-hub-local-proxy",
                "headers": {
                    "x-agent-hub-model-binding-id": binding.id.to_string()
                },
                "models": [Value::Object(model)]
            }
        }
    }))
    .context("serialize Pi models config")
}

fn pi_model_provider_name(binding_id: Uuid) -> String {
    format!("agent-hub-{}", binding_id.simple())
}

fn pi_model_max_output_tokens(settings: &AgentModelSettings) -> u32 {
    match &settings.request_settings {
        ModelRequestSettings::OpenaiResponses { .. } => None,
        ModelRequestSettings::OpenaiChatCompletions {
            max_completion_tokens,
            ..
        } => *max_completion_tokens,
        ModelRequestSettings::AnthropicMessages { max_tokens, .. } => *max_tokens,
    }
    .unwrap_or(16_384)
}

fn pi_thinking_level(effort: ReasoningEffort) -> Option<&'static str> {
    match effort {
        ReasoningEffort::Default => None,
        ReasoningEffort::None => Some("off"),
        ReasoningEffort::Minimal => Some("minimal"),
        ReasoningEffort::Low => Some("low"),
        ReasoningEffort::Medium => Some("medium"),
        ReasoningEffort::High => Some("high"),
        ReasoningEffort::Xhigh => Some("xhigh"),
        ReasoningEffort::Max | ReasoningEffort::Ultra => Some("max"),
    }
}

fn pi_thinking_level_map(effort: ReasoningEffort, _thinking_level: &str) -> Value {
    match effort {
        ReasoningEffort::None => json!({
            "off": "none",
            "minimal": null,
            "low": null,
            "medium": null,
            "high": null,
            "xhigh": null,
            "max": null
        }),
        ReasoningEffort::Ultra => json!({
            "off": null,
            "minimal": "minimal",
            "low": "low",
            "medium": "medium",
            "high": "high",
            "xhigh": "xhigh",
            "max": "ultra"
        }),
        ReasoningEffort::Minimal
        | ReasoningEffort::Low
        | ReasoningEffort::Medium
        | ReasoningEffort::High
        | ReasoningEffort::Xhigh
        | ReasoningEffort::Max => json!({
            "off": null,
            "minimal": "minimal",
            "low": "low",
            "medium": "medium",
            "high": "high",
            "xhigh": "xhigh",
            "max": "max"
        }),
        ReasoningEffort::Default => Value::Object(serde_json::Map::new()),
    }
}

fn pi_execution_materialization_sha256(
    agent_dir: &Path,
    skill_dirs: &[String],
) -> anyhow::Result<String> {
    let mut paths = ["AGENTS.md", "models.json", "skills-manifest.json"]
        .into_iter()
        .map(|path| agent_dir.join(path))
        .collect::<Vec<_>>();
    for directory in skill_dirs {
        paths.extend(
            WalkDir::new(agent_dir.join("skills").join(directory))
                .follow_links(false)
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|entry| entry.into_path()),
        );
    }
    paths.sort();
    let mut digest = Sha256::new();
    for path in paths {
        let relative = path.strip_prefix(agent_dir)?;
        let metadata = stdfs::symlink_metadata(&path)?;
        digest.update(relative.to_string_lossy().as_bytes());
        digest.update([0]);
        if metadata.is_dir() {
            digest.update(b"directory");
        } else if metadata.is_file() {
            digest.update(stdfs::read(&path)?);
        } else {
            anyhow::bail!("materialized Pi configuration contains an unsupported file type");
        }
        digest.update([0]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn pi_materialization_is_current(
    agent_dir: &Path,
    desired: &PiExecutionMaterializationMarker,
) -> bool {
    let marker_path = agent_dir.join(PI_MATERIALIZATION_MARKER_FILE);
    let Ok(marker) = stdfs::read(&marker_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<PiExecutionMaterializationMarker>(&bytes).ok())
        .ok_or(())
    else {
        return false;
    };
    if marker != *desired
        || !private_file_permissions_are_valid(&marker_path)
        || !private_file_permissions_are_valid(&agent_dir.join("AGENTS.md"))
        || !private_file_permissions_are_valid(&agent_dir.join("models.json"))
        || !private_file_permissions_are_valid(&agent_dir.join("skills-manifest.json"))
        || desired
            .owned_skill_directories
            .iter()
            .any(|directory| !is_single_normal_path_component(directory))
    {
        return false;
    }
    pi_execution_materialization_sha256(agent_dir, &desired.owned_skill_directories)
        .is_ok_and(|digest| digest == desired.materialization_sha256)
}

fn validated_pi_execution_materialization(
    agent_dir: &Path,
) -> anyhow::Result<PiExecutionMaterializationMarker> {
    let marker: PiExecutionMaterializationMarker = serde_json::from_slice(
        &stdfs::read(agent_dir.join(PI_MATERIALIZATION_MARKER_FILE))
            .context("read current Pi execution configuration marker")?,
    )
    .context("parse current Pi execution configuration marker")?;
    anyhow::ensure!(
        marker.format_version == 1 && pi_materialization_is_current(agent_dir, &marker),
        "current per-Session Pi configuration materialization is invalid"
    );
    Ok(marker)
}

async fn commit_pi_execution_materialization(
    agent_dir: &Path,
    stage_agent_dir: &Path,
    marker: &PiExecutionMaterializationMarker,
) -> anyhow::Result<()> {
    fs::create_dir_all(agent_dir.join("skills")).await?;
    let previous_owned = previous_pi_owned_skill_directories(agent_dir);
    for directory in &marker.owned_skill_directories {
        let target = agent_dir.join("skills").join(directory);
        remove_materialized_path(&target).await?;
        fs::rename(stage_agent_dir.join("skills").join(directory), &target)
            .await
            .with_context(|| format!("install managed Pi Skill directory {directory}"))?;
    }
    for directory in previous_owned {
        if !marker.owned_skill_directories.contains(&directory) {
            remove_materialized_path(&agent_dir.join("skills").join(directory)).await?;
        }
    }
    for filename in ["AGENTS.md", "models.json", "skills-manifest.json"] {
        fs::rename(stage_agent_dir.join(filename), agent_dir.join(filename))
            .await
            .with_context(|| format!("install Pi {filename}"))?;
    }
    fs::rename(
        stage_agent_dir.join(PI_MATERIALIZATION_MARKER_FILE),
        agent_dir.join(PI_MATERIALIZATION_MARKER_FILE),
    )
    .await
    .context("commit Pi execution configuration marker")?;
    Ok(())
}

fn previous_pi_owned_skill_directories(agent_dir: &Path) -> Vec<String> {
    if let Ok(marker) = stdfs::read(agent_dir.join(PI_MATERIALIZATION_MARKER_FILE))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<PiExecutionMaterializationMarker>(&bytes).ok())
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
    stdfs::read(agent_dir.join("skills-manifest.json"))
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

#[cfg(unix)]
fn private_file_permissions_are_valid(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    stdfs::metadata(path).is_ok_and(|metadata| metadata.permissions().mode() & 0o777 == 0o600)
}

#[cfg(not(unix))]
fn private_file_permissions_are_valid(_path: &Path) -> bool {
    false
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

fn model_binding<'a>(
    configuration: &'a AgentExecutionConfigurationDto,
    binding_key: &str,
) -> anyhow::Result<&'a RunModelBindingDto> {
    configuration
        .model_bindings
        .iter()
        .find(|binding| binding.binding_key.eq_ignore_ascii_case(binding_key))
        .context("required Run Model Binding is missing from execution configuration")
}

async fn materialize_skills(
    engine_state_root: &Path,
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
        let skill_dir = engine_state_root
            .join("skills")
            .join(skill_directory_name(name));
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

async fn materialize_local_skills(
    engine_state_root: &Path,
    source_root: &Path,
) -> anyhow::Result<()> {
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
        let destination = engine_state_root
            .join("skills")
            .join(skill_directory_name(&name));
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

fn fake_engine_events(claim: &ClaimRunResponse) -> (Vec<AppendRunEventRequest>, String) {
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
                    content: Some("initialized fake Execution Engine".into()),
                    payload: json!({ "phase": "initialize" }),
                    waiting_tool: None,
                },
                AppendRunEventRequest {
                    event_type: "tool_request".into(),
                    role: Some("assistant".into()),
                    content: Some(format!("Fake Execution Engine requested {tool_name} tool")),
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
            "Fake Execution Engine completed integration tool result for agent '{}'. {}. Result: {}",
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
            "Fake Execution Engine completed run for agent '{}'. Instructions loaded: {}. User said: {}",
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
                content: Some("initialized fake Execution Engine".into()),
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
            native_session_id: Some("thread-7".into()),
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
        config.engine_driver = "fake".into();
        config.work_root = temp.path().to_path_buf();
        let mut claim = test_claim();
        claim.run.integration_session_id = Some(Uuid::new_v4());
        claim.run.source = "integration:message".into();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([{ "name": "echo" }]),
            attachments: json!([]),
            tool_result: None,
            external_user: None,
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
            Some("native-session-for-tools"),
            "/runtime/workdir",
        )
        .unwrap()
        .expect("a waiting turn should produce one finalize batch");

        assert_eq!(batch.integration_session_id, integration_session_id);
        assert_eq!(batch.native_session_id, "native-session-for-tools");
        assert_eq!(batch.work_dir_ref, "/runtime/workdir");
        assert_eq!(batch.tool_requests.len(), 2);
        assert_eq!(batch.tool_requests[0].payload, first.payload);
        assert_eq!(batch.tool_requests[1].payload, second.payload);
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
        config.engine_driver = "fake".into();
        config.work_root = temp.path().to_path_buf();
        let mut claim = test_claim();
        claim.run.integration_session_id = Some(Uuid::new_v4());
        claim.run.source = "integration:message".into();
        claim.run.initial_message = "use the tool".into();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([{ "name": "echo" }]),
            attachments: json!([]),
            tool_result: None,
            external_user: None,
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
        config.engine_driver = "fake".into();
        config.work_root = temp.path().to_path_buf();
        let mut claim = test_claim();
        claim.run.integration_session_id = Some(Uuid::new_v4());
        claim.run.source = "integration:message".into();
        claim.run.initial_message = "use the tool".into();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([{ "name": "echo" }]),
            attachments: json!([]),
            tool_result: None,
            external_user: None,
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
            native_session_id: "response-loss-native-session".into(),
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
        let first_agent_dir = pi_agent_directory(&first.engine_state_root);
        fs::write(first_agent_dir.join("skills/stale.md"), "old")
            .await
            .unwrap();

        let mut later_run = claim.clone();
        later_run.run.id = Uuid::new_v4();
        let second = prepare_run_env(temp.path(), &later_run, None)
            .await
            .unwrap();

        assert_eq!(first.workdir, second.workdir);
        assert_eq!(first.engine_state_root, second.engine_state_root);
        assert_eq!(
            second.workdir,
            temp.path()
                .join("sessions")
                .join(session_id.to_string())
                .join("workspace")
        );
        assert_eq!(
            second.engine_state_root,
            temp.path()
                .join("sessions")
                .join(session_id.to_string())
                .join("engine-state")
        );
        assert!(second.workdir.exists());
        let second_agent_dir = pi_agent_directory(&second.engine_state_root);
        assert!(second_agent_dir.join("models.json").exists());
        assert!(second_agent_dir.join("skills/stale.md").exists());
        assert!(second.workdir.parent().unwrap().join("supervisor").is_dir());
        assert!(second.workdir.parent().unwrap().join("staging").is_dir());
        assert!(second_agent_dir
            .join("skills")
            .join(skill_directory_name("repo-review"))
            .join("SKILL.md")
            .exists());

        let mut other_session = claim;
        other_session.run.id = Uuid::new_v4();
        other_session.run.hub_session_id = Some(Uuid::new_v4());
        let isolated = prepare_run_env(temp.path(), &other_session, None)
            .await
            .unwrap();
        assert_ne!(isolated.workdir, second.workdir);
        assert_ne!(isolated.engine_state_root, second.engine_state_root);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_materialization_skips_valid_state_and_repairs_proxy_or_missing_marker() {
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
        let agent_dir = pi_agent_directory(&first.engine_state_root);
        let models_path = agent_dir.join("models.json");
        let marker_path = agent_dir.join(PI_MATERIALIZATION_MARKER_FILE);
        let first_inode = fs::metadata(&models_path).await.unwrap().ino();
        assert_eq!(
            fs::read_to_string(agent_dir.join("AGENTS.md"))
                .await
                .unwrap(),
            "Task 9 durable guidance\n"
        );
        let models = fs::read_to_string(&models_path).await.unwrap();
        let marker = fs::read_to_string(&marker_path).await.unwrap();
        assert!(!models.contains(MCP_SECRET));
        assert!(!marker.contains(MCP_SECRET));
        assert_eq!(
            fs::metadata(&models_path)
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
        assert_eq!(fs::metadata(&models_path).await.unwrap().ino(), first_inode);

        let _ = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:41002/v1"))
            .await
            .unwrap();
        let proxy_inode = fs::metadata(&models_path).await.unwrap().ino();
        assert_ne!(proxy_inode, first_inode);
        assert!(fs::read_to_string(&models_path)
            .await
            .unwrap()
            .contains("41002"));

        fs::remove_file(&marker_path).await.unwrap();
        let _ = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:41002/v1"))
            .await
            .unwrap();
        assert_ne!(fs::metadata(&models_path).await.unwrap().ino(), proxy_inode);
        assert!(marker_path.exists());
    }

    #[tokio::test]
    async fn online_refresh_without_bindings_preserves_pi_provider_routes_until_the_next_run() {
        let temp = tempfile::tempdir().unwrap();
        let runtime_id = Uuid::new_v4();
        let mut first = test_claim();
        first.run.runtime_id = Some(runtime_id);
        first.execution_configuration.instructions = "old guidance".into();
        let reviewer_connection_id = Uuid::from_u128(0x301);
        let reviewer_binding_id = Uuid::from_u128(0x302);
        first.execution_configuration.subagents = vec![SubagentDefinition {
            name: "reviewer".into(),
            description: "Review the current change".into(),
            developer_instructions: "Use the old review guidance.".into(),
            model_selection: Some(ModelSelectionDto {
                connection_id: reviewer_connection_id,
                model_id: "gpt-review-old".into(),
            }),
            model_settings_override: AgentModelSettingsOverride {
                reasoning_effort: ModelSettingOverride::Value(ReasoningEffort::Ultra),
                ..AgentModelSettingsOverride::default()
            },
            enabled: true,
            disabled_reason: None,
        }];
        first
            .execution_configuration
            .model_bindings
            .push(RunModelBindingDto {
                id: reviewer_binding_id,
                run_id: first.run.id,
                binding_key: "reviewer".into(),
                model_connection_id: reviewer_connection_id,
                connection_name_snapshot: "Old reviewer".into(),
                connection_scope_snapshot: ModelConnectionScope::Global,
                model_id: "gpt-review-old".into(),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                model_settings: AgentModelSettings {
                    reasoning_effort: ReasoningEffort::Ultra,
                    ..AgentModelSettings::default()
                },
            });
        first.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&first.execution_configuration).unwrap();
        let run_env = prepare_run_env(temp.path(), &first, Some("http://127.0.0.1:4567/v1"))
            .await
            .unwrap();
        let agent_dir = run_env.engine_state_root.join(PI_AGENT_DIRECTORY);
        let original_models = fs::read_to_string(agent_dir.join("models.json"))
            .await
            .unwrap()
            .parse::<Value>()
            .unwrap();

        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);
        manager.reserve_claim(&first).unwrap();
        manager.complete_fake_claim(&first).await.unwrap();

        let mut refreshed = first.execution_configuration.clone();
        refreshed.revision += 1;
        refreshed.instructions = "refreshed non-model guidance".into();
        refreshed.model_selection = None;
        refreshed.model_settings = AgentModelSettings::default();
        refreshed.model_bindings.clear();
        refreshed.mcp_allowlist = json!([{
            "name": "refresh-mcp",
            "command": "refresh-command",
            "args": ["--new"]
        }]);
        refreshed.subagents[0].developer_instructions = "Use refreshed review guidance.".into();
        refreshed.subagents[0].model_selection = None;
        refreshed.subagents[0].model_settings_override = AgentModelSettingsOverride::default();
        let refreshed_fingerprint = execution_configuration_fingerprint(&refreshed).unwrap();
        let command = RuntimeSessionCommandDto {
            command_id: Uuid::new_v4(),
            session_id: first.run.hub_session_id.unwrap(),
            ownership_generation: 1,
            command: "refresh_configuration".into(),
            run_id: None,
            turn_id: None,
            native_session_id: None,
            native_turn_id: None,
            message: None,
            configuration_revision: Some(refreshed.revision),
            fingerprint: Some(refreshed_fingerprint.clone()),
            execution_configuration: Some(refreshed),
        };

        let applied = apply_runtime_session_command(&manager, &command, None)
            .await
            .unwrap();

        assert_eq!(applied.outcome, "applied");
        assert!(applied.native_error.is_none());
        let refreshed_models = fs::read_to_string(agent_dir.join("models.json"))
            .await
            .unwrap()
            .parse::<Value>()
            .unwrap();
        assert_eq!(refreshed_models, original_models);
        assert_eq!(
            fs::read_to_string(agent_dir.join("AGENTS.md"))
                .await
                .unwrap(),
            "refreshed non-model guidance\n"
        );
        assert!(!agent_dir.join("agents").exists());
        assert!(!agent_dir.join("mcp-allowlist.json").exists());
        let marker: PiExecutionMaterializationMarker = serde_json::from_slice(
            &fs::read(agent_dir.join(PI_MATERIALIZATION_MARKER_FILE))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(marker.configuration_fingerprint, refreshed_fingerprint);

        let mut next = first.clone();
        next.run.id = Uuid::new_v4();
        next.execution_configuration.revision += 1;
        next.execution_configuration.instructions = "refreshed non-model guidance".into();
        next.execution_configuration.mcp_allowlist = json!([{
            "name": "refresh-mcp",
            "command": "refresh-command",
            "args": ["--new"]
        }]);
        let next_binding_id = Uuid::from_u128(0x401);
        next.execution_configuration.model_selection = Some(ModelSelectionDto {
            connection_id: next.execution_configuration.model_bindings[0].model_connection_id,
            model_id: "gpt-main-next".into(),
        });
        next.execution_configuration.model_settings = AgentModelSettings {
            reasoning_effort: ReasoningEffort::High,
            ..AgentModelSettings::default()
        };
        next.execution_configuration.model_bindings[0].id = next_binding_id;
        next.execution_configuration.model_bindings[0].run_id = next.run.id;
        next.execution_configuration.model_bindings[0].model_id = "gpt-main-next".into();
        next.execution_configuration.model_bindings[0].model_settings =
            next.execution_configuration.model_settings.clone();
        next.execution_configuration.model_bindings[1].id = Uuid::from_u128(0x402);
        next.execution_configuration.model_bindings[1].run_id = next.run.id;
        next.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&next.execution_configuration).unwrap();

        let next_env = prepare_run_env(temp.path(), &next, Some("http://127.0.0.1:4567/v1"))
            .await
            .unwrap();
        let next_models = fs::read_to_string(
            next_env
                .engine_state_root
                .join(PI_AGENT_DIRECTORY)
                .join("models.json"),
        )
        .await
        .unwrap();
        assert!(next_models.contains("gpt-main-next"));
        assert!(next_models.contains(&next_binding_id.to_string()));
        manager.shutdown();
    }

    #[tokio::test]
    async fn pi_materialization_removes_owned_skills_preserves_unknown_and_isolates_sessions() {
        let temp = tempfile::tempdir().unwrap();
        let claim = test_claim();
        let first = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let first_agent_dir = pi_agent_directory(&first.engine_state_root);
        let removed_skill = first_agent_dir
            .join("skills")
            .join(skill_directory_name("repo-review"));
        let unknown_skill = first_agent_dir.join("skills/.system/plugin-owned");
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
        let other_agent_dir = pi_agent_directory(&other.engine_state_root);
        let other_skill_dir = skill_directory_name("other-session");

        assert!(other_agent_dir
            .join("skills")
            .join(&other_skill_dir)
            .join("SKILL.md")
            .exists());
        assert!(!first_agent_dir
            .join("skills")
            .join(&other_skill_dir)
            .exists());
        assert!(!other_agent_dir.join("skills/.system").exists());
    }

    #[tokio::test]
    async fn pi_materialization_staging_failure_preserves_previous_state_and_can_retry() {
        let temp = tempfile::tempdir().unwrap();
        let mut original = test_claim();
        original.execution_configuration.instructions = "original guidance".into();
        original.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&original.execution_configuration).unwrap();
        let env = prepare_run_env(temp.path(), &original, None).await.unwrap();
        let agent_dir = pi_agent_directory(&env.engine_state_root);
        let guidance_path = agent_dir.join("AGENTS.md");
        let marker_path = agent_dir.join(PI_MATERIALIZATION_MARKER_FILE);
        let original_guidance = fs::read(&guidance_path).await.unwrap();
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
            "failed synchronization left a Pi execution configuration staging directory"
        );
        assert_eq!(fs::read(&guidance_path).await.unwrap(), original_guidance);
        assert_eq!(fs::read(&marker_path).await.unwrap(), original_marker);

        let retried = prepare_run_env(temp.path(), &updated, None).await.unwrap();
        let retried_agent_dir = pi_agent_directory(&retried.engine_state_root);
        assert_eq!(
            fs::read_to_string(retried_agent_dir.join("AGENTS.md"))
                .await
                .unwrap(),
            "updated guidance\n"
        );
        let marker: PiExecutionMaterializationMarker = serde_json::from_slice(
            &fs::read(retried_agent_dir.join(PI_MATERIALIZATION_MARKER_FILE))
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
        let native_session_id = "019bf9b2-7a4d-7000-8000-000000000004";
        let now = chrono::Utc::now();
        claim.session_context = Some(ClaimSessionContextDto {
            session: HubSessionDto {
                id: session_id,
                owner_id: claim.agent.owner_id,
                agent_id: claim.agent.id,
                agent_name: claim.agent.name.clone(),
                agent_deleted_at: None,
                origin_platform_name: None,
                origin: HubSessionOriginDto::HubNative,
                lifecycle_status: "online".into(),
                native_session_id: Some(native_session_id.into()),
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
        fs::create_dir_all(paths.engine_state.join("sessions"))
            .await
            .unwrap();
        fs::write(
            paths.engine_state.join("sessions/pi-session.jsonl"),
            format!("{{\"type\":\"session\",\"id\":\"{native_session_id}\"}}\n"),
        )
        .await
        .unwrap();
        fs::write(
            paths
                .engine_state
                .join(format!("sessions/decoy-{native_session_id}.jsonl")),
            "{\"type\":\"session\",\"id\":\"another-session\"}\n",
        )
        .await
        .unwrap();
        fs::create_dir_all(paths.engine_state.join(".pi/agent/skills/private"))
            .await
            .unwrap();
        fs::create_dir_all(paths.engine_state.join(".pi/agent/extensions"))
            .await
            .unwrap();
        fs::create_dir_all(paths.engine_state.join(".pi/agent/cache"))
            .await
            .unwrap();
        for (relative, contents) in [
            (".pi/agent/models.json", "model proxy token"),
            (".pi/agent/auth.json", "provider secret"),
            (".pi/agent/settings.json", "settings"),
            (".pi/agent/skills/private/SKILL.md", "generated skill"),
            (".pi/agent/extensions/provider.ts", "generated extension"),
            (".pi/agent/cache/data", "cache"),
        ] {
            fs::write(paths.engine_state.join(relative), contents)
                .await
                .unwrap();
        }
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
            producing_engine_version: "0.104.0".into(),
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
        let recovered =
            pi_driver::discover_session_file(&restored.join("engine-state"), native_session_id)
                .unwrap();
        assert_eq!(
            recovered.file_name().unwrap().to_string_lossy(),
            "pi-session.jsonl"
        );
        assert!(!restored.join("engine-state/.pi").exists());
        assert!(!restored
            .join(format!(
                "engine-state/sessions/decoy-{native_session_id}.jsonl"
            ))
            .exists());
        hub.abort();
    }

    #[tokio::test]
    async fn restoring_claim_installs_the_current_bundle_and_resumes_its_pi_session() {
        let temp = tempfile::tempdir().unwrap();
        let source_workspace = temp.path().join("source/workspace");
        let source_engine_state = temp.path().join("source/engine-state");
        fs::create_dir_all(&source_workspace).await.unwrap();
        fs::create_dir_all(source_engine_state.join("sessions"))
            .await
            .unwrap();
        fs::write(source_workspace.join("restored.txt"), "from bundle\n")
            .await
            .unwrap();
        let native_session_id = "fake-pi-restored";
        fs::write(
            source_engine_state.join("sessions/restored.jsonl"),
            format!("{{\"type\":\"session\",\"id\":\"{native_session_id}\"}}\n"),
        )
        .await
        .unwrap();
        fs::write(
            source_engine_state.join("sessions/other.jsonl"),
            "{\"type\":\"session\",\"id\":\"another-session\"}\n",
        )
        .await
        .unwrap();
        fs::create_dir_all(source_engine_state.join(".pi/agent/skills/private"))
            .await
            .unwrap();
        fs::create_dir_all(source_engine_state.join(".pi/agent/extensions"))
            .await
            .unwrap();
        fs::create_dir_all(source_engine_state.join(".pi/agent/cache"))
            .await
            .unwrap();
        for (relative, contents) in [
            (".pi/agent/models.json", "must not restore"),
            (".pi/agent/auth.json", "must not restore"),
            (".pi/agent/settings.json", "must not restore"),
            (".pi/agent/skills/private/SKILL.md", "must not restore"),
            (".pi/agent/extensions/provider.ts", "must not restore"),
            (".pi/agent/cache/data", "must not restore"),
        ] {
            fs::write(source_engine_state.join(relative), contents)
                .await
                .unwrap();
        }
        fs::write(
            source_engine_state.join("session_index.jsonl"),
            "must not restore",
        )
        .await
        .unwrap();
        let session_id = Uuid::new_v4();
        let created_at = chrono::Utc::now();
        let artifact =
            session_bundle::create_session_bundle(&session_bundle::SessionBundleCreateSpec {
                session_id,
                native_session_id: native_session_id.into(),
                history_checkpoint: 8,
                bundle_generation: 2,
                ownership_generation: 3,
                producing_engine_version: "0.103.0".into(),
                created_at,
                workspace: source_workspace,
                engine_state_root: source_engine_state,
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
                origin_platform_name: None,
                origin: HubSessionOriginDto::HubNative,
                lifecycle_status: "restoring".into(),
                native_session_id: Some(native_session_id.into()),
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
                    producing_engine_version: "0.103.0".into(),
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
        let restored_session =
            pi_driver::discover_session_file(&paths.engine_state, native_session_id).unwrap();
        assert_eq!(
            restored_session.file_name().unwrap().to_string_lossy(),
            "restored.jsonl"
        );
        assert!(!paths.engine_state.join("sessions/other.jsonl").exists());
        assert!(!paths.engine_state.join(".pi").exists());
        assert!(!paths.engine_state.join("session_index.jsonl").exists());
        let metadata: SessionSupervisorMetadata = serde_json::from_slice(
            &fs::read(paths.supervisor.join(SESSION_SUPERVISOR_METADATA_FILE))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata.runtime_id, runtime_id);
        assert_eq!(metadata.ownership_generation, 4);
        assert_eq!(
            metadata.native_session_id.as_deref(),
            Some(native_session_id)
        );

        let run_env = prepare_run_env(&config.work_root, &claim, None)
            .await
            .unwrap();
        let pid_file = temp.path().join("pi-restored.pid");
        let pi_bin = write_fake_pi_wrapper(&temp, &pid_file, &["FAKE_PI_DISABLE_MODEL=1"]);
        let mut process = pi_driver::PersistentPiRpcProcess::start(
            pi_bin.to_str().unwrap(),
            &run_env,
            Some(&restored_session),
            &pi_driver::pi_tool_allowlist(&claim.agent),
            Duration::from_secs(2),
            Arc::new(EngineCancellation::default()),
        )
        .unwrap();
        assert_eq!(process.native_session_id(), native_session_id);
        assert_eq!(
            process.execute(&claim, None).unwrap().final_status,
            "completed"
        );
        drop(process);
        assert_process_group_reaped_or_clean_up(&pid_file);
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
                origin_platform_name: None,
                origin: HubSessionOriginDto::HubNative,
                lifecycle_status: "restoring".into(),
                native_session_id: Some("native-thread-invalid-bundle".into()),
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
                    producing_engine_version: "0.103.0".into(),
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
            session_supervisor_metadata_for_claim(runtime_id, &claim, "test-engine").unwrap();
        metadata.idle_deadline_unix_ms = Some(10_000);
        persist_session_supervisor_metadata(temp.path(), &metadata)
            .await
            .unwrap();
        let snapshots = vec![RuntimeOwnedSessionSnapshotDto {
            session_id: claim.run.hub_session_id.unwrap(),
            ownership_generation: 1,
            lifecycle_status: "online".into(),
            native_session_id: None,
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
                native_session_id: None,
                native_turn_id: None,
                message: None,
                configuration_revision: None,
                fingerprint: None,
                execution_configuration: None,
            }],
            None,
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

        assert!(
            send_runtime_heartbeat(&config, &client, &mut stored, Some(&manager))
                .await
                .is_err()
        );
        assert_eq!(manager.heartbeat_request().cleaned_sessions, vec![expected]);
        send_runtime_heartbeat(&config, &client, &mut stored, Some(&manager))
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
            producing_engine_version: "test-engine".into(),
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
            session_supervisor_metadata_for_claim(runtime_id, &claim, "test-engine").unwrap();
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
            native_session_id: None,
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
    async fn tampered_pi_owned_skill_marker_cannot_delete_outside_engine_state_root() {
        let temp = tempfile::tempdir().unwrap();
        let original = test_claim();
        let env = prepare_run_env(temp.path(), &original, None).await.unwrap();
        let sentinel = env.workdir.join("must-survive.txt");
        fs::write(&sentinel, "workspace state").await.unwrap();

        let agent_dir = pi_agent_directory(&env.engine_state_root);
        let marker_path = agent_dir.join(PI_MATERIALIZATION_MARKER_FILE);
        let mut marker: PiExecutionMaterializationMarker =
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
        let repaired: PiExecutionMaterializationMarker =
            serde_json::from_slice(&fs::read(&marker_path).await.unwrap()).unwrap();
        assert_eq!(
            repaired.owned_skill_directories,
            vec![skill_directory_name("repo-review")]
        );
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
                native_session_id: None,
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
                native_session_id: None,
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

    #[test]
    fn fake_engine_emits_assistant_message() {
        let claim = test_claim();
        let (events, status) = fake_engine_events(&claim);
        assert_eq!(status, "completed");
        assert!(events
            .iter()
            .any(|event| event.role.as_deref() == Some("assistant")));
    }

    #[test]
    fn invalid_engine_driver_is_rejected() {
        assert!(validate_engine_driver("fake").is_ok());
        assert!(validate_engine_driver("pi").is_ok());
        assert!(validate_engine_driver("typo").is_err());
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
    fn pi_models_json_uses_the_local_gateway_and_preserves_ultra_intent() {
        let mut claim = test_claim();
        let binding = claim
            .execution_configuration
            .model_bindings
            .first_mut()
            .unwrap();
        binding.api_type = ModelUpstreamProtocol::OpenaiChatCompletions;
        binding.model_settings = AgentModelSettings {
            reasoning_effort: ReasoningEffort::Ultra,
            context_window_tokens: Some(200_000),
            request_settings: ModelRequestSettings::OpenaiChatCompletions {
                temperature: None,
                top_p: None,
                max_completion_tokens: Some(12_345),
            },
            ..AgentModelSettings::default()
        };

        let rendered = render_pi_models_json(
            &claim.execution_configuration,
            Some("http://127.0.0.1:4567/v1"),
        )
        .unwrap();
        let parsed: Value = serde_json::from_str(&rendered).unwrap();
        let binding = &claim.execution_configuration.model_bindings[0];
        let provider_name = pi_model_provider_name(binding.id);
        let provider = &parsed["providers"][&provider_name];
        let model = &provider["models"][0];

        assert_eq!(provider["baseUrl"], "http://127.0.0.1:4567/v1");
        assert_eq!(provider["api"], "openai-responses");
        assert_eq!(
            provider["headers"]["x-agent-hub-model-binding-id"],
            binding.id.to_string()
        );
        assert_eq!(provider["apiKey"], "agent-hub-local-proxy");
        assert!(provider.get("x-agent-hub-model-connection-id").is_none());
        assert_eq!(model["id"], "gpt-main");
        assert_eq!(model["reasoning"], true);
        assert_eq!(model["contextWindow"], 200_000);
        assert_eq!(model["maxTokens"], 12_345);
        assert_eq!(model["thinkingLevelMap"]["max"], "ultra");
        assert_eq!(pi_thinking_level(ReasoningEffort::Ultra), Some("max"));
        assert_eq!(pi_thinking_level(ReasoningEffort::Default), None);
        assert_eq!(pi_thinking_level(ReasoningEffort::None), Some("off"));
    }

    #[tokio::test]
    async fn pi_materialization_is_private_skill_aware_and_excludes_mcp_and_subagents() {
        const PROVIDER_URL: &str = "https://provider-secret.example";
        const PROVIDER_API_KEY: &str = "provider-api-key-must-not-leak";
        const MCP_SECRET: &str = "mcp-secret-must-not-leak";

        let temp = tempfile::tempdir().unwrap();
        let mut claim = test_claim();
        claim.execution_configuration.model_policy = json!({
            "base_url": PROVIDER_URL,
            "api_key": PROVIDER_API_KEY,
        });
        claim.execution_configuration.mcp_allowlist = json!([{
            "name": "private-mcp",
            "command": "private-mcp",
            "secrets": { "TOKEN": MCP_SECRET }
        }]);
        claim.execution_configuration.subagents = vec![SubagentDefinition {
            name: "excluded".into(),
            description: "must not be materialized for Pi".into(),
            developer_instructions: "do not run".into(),
            model_selection: None,
            model_settings_override: AgentModelSettingsOverride::default(),
            enabled: true,
            disabled_reason: None,
        }];
        claim.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap();
        let paths = SessionPaths::for_claim(temp.path(), &claim).unwrap();
        fs::create_dir_all(&paths.workspace).await.unwrap();
        fs::create_dir_all(&paths.engine_state).await.unwrap();
        fs::create_dir_all(&paths.staging).await.unwrap();

        synchronize_pi_execution_configuration(
            &paths,
            &claim.execution_configuration,
            &claim.expected_configuration_fingerprint,
            PiModelConfigurationMaterialization::RunBindings {
                model_base_url: Some("http://127.0.0.1:4567/v1"),
            },
            None,
        )
        .await
        .unwrap();

        let agent_dir = paths.engine_state.join(".pi/agent");
        assert_eq!(
            fs::read_to_string(agent_dir.join("AGENTS.md"))
                .await
                .unwrap(),
            "Be concise\n"
        );
        let skill_directory = skill_directory_name("repo-review");
        assert!(agent_dir
            .join("skills")
            .join(&skill_directory)
            .join("SKILL.md")
            .is_file());
        assert!(!agent_dir.join("mcp-allowlist.json").exists());
        assert!(!agent_dir.join("agents").exists());
        assert!(private_file_permissions_are_valid(
            &agent_dir.join("AGENTS.md")
        ));
        assert!(private_file_permissions_are_valid(
            &agent_dir.join("models.json")
        ));
        assert!(private_file_permissions_are_valid(
            &agent_dir.join(PI_MATERIALIZATION_MARKER_FILE)
        ));

        for entry in WalkDir::new(&agent_dir) {
            let entry = entry.unwrap();
            if !entry.file_type().is_file() {
                continue;
            }
            let bytes = stdfs::read(entry.path()).unwrap();
            let contents = String::from_utf8_lossy(&bytes);
            assert!(!contents.contains(PROVIDER_URL));
            assert!(!contents.contains(PROVIDER_API_KEY));
            assert!(!contents.contains(MCP_SECRET));
        }

        let mut refreshed = claim.execution_configuration.clone();
        refreshed.skills.clear();
        let refreshed_fingerprint = execution_configuration_fingerprint(&refreshed).unwrap();
        synchronize_pi_execution_configuration(
            &paths,
            &refreshed,
            &refreshed_fingerprint,
            PiModelConfigurationMaterialization::PreserveExisting,
            None,
        )
        .await
        .unwrap();
        assert!(!agent_dir.join("skills").join(skill_directory).exists());
    }

    #[tokio::test]
    async fn persistent_pi_rpc_process_translates_fixture_events_and_suppresses_duplicates() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pi.pid");
        let pi_bin = write_fake_pi_wrapper(
            &temp,
            &pid_file,
            &["FAKE_PI_DISABLE_MODEL=1", "FAKE_PI_DUPLICATE_EVENTS=1"],
        );
        let mut claim = test_claim();
        claim.run.initial_message = "fixture:retry".into();
        let run_env = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:1/v1"))
            .await
            .unwrap();
        let cancellation = Arc::new(EngineCancellation::default());
        let mut process = pi_driver::PersistentPiRpcProcess::start(
            pi_bin.to_str().unwrap(),
            &run_env,
            None,
            &pi_driver::pi_tool_allowlist(&claim.agent),
            Duration::from_secs(2),
            cancellation,
        )
        .unwrap();

        assert!(!process.native_session_id().is_empty());
        assert!(process.session_file().is_file());
        let result = process.execute(&claim, None).unwrap();
        assert_eq!(result.final_status, "completed");
        assert_eq!(
            result.native_session_id.as_deref(),
            Some(process.native_session_id())
        );
        assert_eq!(
            result.native_turn_id.as_deref(),
            Some(claim.run.id.to_string().as_str())
        );
        assert_eq!(
            result
                .events
                .iter()
                .filter(|event| event.event_type == "turn_started")
                .count(),
            1
        );
        assert_eq!(
            result
                .events
                .iter()
                .filter(|event| {
                    event.event_type == "item"
                        && event.payload["item_id"] == "fake-pi-bash-1"
                        && event.payload["phase"] == "started"
                })
                .count(),
            1
        );
        assert_eq!(
            result
                .events
                .iter()
                .filter(|event| {
                    event.event_type == "item"
                        && event.payload["item_id"] == "fake-pi-bash-1"
                        && event.payload["phase"] == "completed"
                })
                .count(),
            1
        );
        assert!(result.events.iter().any(|event| {
            event.event_type == "item"
                && event.payload["item_type"] == "reasoning"
                && event.payload["phase"] == "summary_delta"
        }));
        assert!(result.events.iter().any(|event| {
            event.event_type == "message"
                && event.content.as_deref() == Some("Fake Pi response for: fixture:retry")
        }));
        assert!(result.events.iter().any(|event| {
            event.event_type == "item"
                && event.payload["item_type"] == "contextCompaction"
                && event.payload["phase"] == "started"
        }));
        assert!(result.events.iter().any(|event| {
            event.event_type == "item"
                && event.payload["item_type"] == "retry"
                && event.payload["phase"] == "completed"
        }));
        let serialized = serde_json::to_string(&result.events).unwrap();
        assert!(!serialized.contains("fixture-sensitive"));

        drop(process);
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn persistent_pi_rpc_process_maps_integration_tools_to_waiting_requests() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pi-integration-tool.pid");
        let pi_bin = write_fake_pi_wrapper(&temp, &pid_file, &["FAKE_PI_DISABLE_MODEL=1"]);
        let mut claim = test_claim();
        claim.run.initial_message = "fixture:integration".into();
        claim.run.integration_session_id = Some(Uuid::new_v4());
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([{
                "name": "echo",
                "description": "Echo integration input",
                "parameters": { "type": "object" }
            }]),
            attachments: json!([{
                "kind": "text",
                "name": "qa-note.txt",
                "content_type": "text/plain",
                "size_bytes": 32,
                "text": "quoted text, arrays [1, 2], and a second line\nkept exactly",
                "url": null
            }]),
            tool_result: None,
            external_user: None,
        });
        let run_env = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:1/v1"))
            .await
            .unwrap();
        let mut process = pi_driver::PersistentPiRpcProcess::start(
            pi_bin.to_str().unwrap(),
            &run_env,
            None,
            &pi_driver::pi_tool_allowlist_for_claim(&claim).unwrap(),
            Duration::from_secs(2),
            Arc::new(EngineCancellation::default()),
        )
        .unwrap();

        let result = process.execute(&claim, None).unwrap();
        assert_eq!(result.final_status, "waiting_tool");
        let requests = result
            .events
            .iter()
            .filter(|event| event.event_type == "tool_request")
            .collect::<Vec<_>>();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].payload["source_id"], "platform|tool-call");
        assert_eq!(requests[0].payload["tool_name"], "echo");
        assert_eq!(
            requests[0].payload["tool_request_id"],
            stable_tool_request_uuid(
                claim.run.id,
                "echo",
                Some("platform|tool-call"),
                &requests[0].payload["arguments"],
            )
            .to_string()
        );
        assert_eq!(
            requests[0].payload["arguments"],
            json!({
                "message": "fixture:integration",
                "attachments": [{
                    "kind": "text",
                    "name": "qa-note.txt",
                    "content_type": "text/plain",
                    "size_bytes": 32,
                    "text": "quoted text, arrays [1, 2], and a second line\nkept exactly",
                    "url": null
                }]
            })
        );
        assert!(!result.events.iter().any(|event| {
            event.event_type == "item" && event.payload["item_id"] == "platform-tool-call"
        }));

        drop(process);
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_integration_tools_materialize_as_data_and_empty_catalog_removes_bridge() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let temp = tempfile::tempdir().unwrap();
        let mut claim = test_claim();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([{
                "name": "echo",
                "description": "Echo quotes like `x` without becoming code.",
                "parameters": {
                    "type": "object",
                    "properties": { "message": { "type": "string" } },
                    "required": ["message"]
                }
            }]),
            attachments: json!([]),
            tool_result: None,
            external_user: None,
        });

        let run_env = prepare_run_env(temp.path(), &claim, None).await.unwrap();
        let agent_dir = run_env.engine_state_root.join(".pi/agent");
        let catalog_path = agent_dir.join("agent-hub-integration-tools.json");
        let extension_path = agent_dir.join("agent-hub-integration-tools.mjs");
        let catalog: Value =
            serde_json::from_slice(&fs::read(&catalog_path).await.unwrap()).unwrap();
        assert_eq!(catalog[0]["name"], "echo");
        assert_eq!(
            catalog[0]["description"],
            "Echo quotes like `x` without becoming code."
        );
        let extension = fs::read_to_string(&extension_path).await.unwrap();
        assert!(extension.contains("pi.registerTool"));
        assert!(!extension.contains("Echo quotes like `x` without becoming code."));
        assert_eq!(
            fs::metadata(&catalog_path)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            pi_driver::pi_tool_allowlist_for_claim(&claim).unwrap(),
            ["read", "grep", "find", "ls", "edit", "write", "bash", "echo"]
        );

        let catalog_sentinel = temp.path().join("catalog-sentinel");
        let extension_sentinel = temp.path().join("extension-sentinel");
        fs::write(&catalog_sentinel, b"catalog sentinel")
            .await
            .unwrap();
        fs::write(&extension_sentinel, b"extension sentinel")
            .await
            .unwrap();
        fs::remove_file(&catalog_path).await.unwrap();
        fs::remove_file(&extension_path).await.unwrap();
        symlink(&catalog_sentinel, &catalog_path).unwrap();
        symlink(&extension_sentinel, &extension_path).unwrap();
        pi_driver::materialize_integration_tools(&run_env, claim.integration_context.as_ref())
            .unwrap();
        assert_eq!(
            fs::read_to_string(&catalog_sentinel).await.unwrap(),
            "catalog sentinel"
        );
        assert_eq!(
            fs::read_to_string(&extension_sentinel).await.unwrap(),
            "extension sentinel"
        );
        assert!(fs::symlink_metadata(&catalog_path)
            .await
            .unwrap()
            .file_type()
            .is_file());
        assert!(fs::symlink_metadata(&extension_path)
            .await
            .unwrap()
            .file_type()
            .is_file());

        claim.integration_context.as_mut().unwrap().tools = json!([{ "name": "bash" }]);
        assert_eq!(
            pi_driver::materialize_integration_tools(&run_env, claim.integration_context.as_ref())
                .unwrap_err()
                .to_string(),
            "Integration tool name conflicts with a Pi built-in tool"
        );
        assert_eq!(
            pi_driver::pi_tool_allowlist_for_claim(&claim)
                .unwrap_err()
                .to_string(),
            "Integration tool name conflicts with a Pi built-in tool"
        );

        claim.run.id = Uuid::new_v4();
        claim.integration_context.as_mut().unwrap().tools = json!([]);
        prepare_run_env(temp.path(), &claim, None).await.unwrap();
        assert!(!catalog_path.exists());
        assert!(!extension_path.exists());
    }

    #[test]
    fn pi_prompt_preserves_structured_integration_attachments_and_tool_result() {
        let mut claim = test_claim();
        claim.run.initial_message = "message with \"quotes\" and a second line\nkept".into();
        claim.integration_context = Some(IntegrationContextDto {
            tools: json!([]),
            attachments: json!([{
                "kind": "url",
                "name": "reference",
                "content_type": "text/html",
                "size_bytes": 0,
                "text": null,
                "url": "https://example.com/reference?source=qa"
            }]),
            tool_result: Some(json!({
                "text": "tool result with \"quotes\"",
                "nested": { "values": [1, true, null, { "line": "first\nsecond" }] }
            })),
            external_user: Some(ExternalUserContextDto {
                external_user_id: "external-42".into(),
                tenant_id: "tenant-7".into(),
                username: Some("ada".into()),
                display_name: Some("Ada Lovelace".into()),
                email: Some("ada@example.com".into()),
                attributes: json!({ "plan": "pro" }),
            }),
        });

        let prompt = pi_driver::pi_prompt_text(&claim).unwrap();
        let (_, encoded) = prompt
            .split_once("Agent Hub Integration context (JSON):\n")
            .unwrap();
        let envelope: Value = serde_json::from_str(encoded).unwrap();
        assert_eq!(envelope["message"], claim.run.initial_message);
        assert_eq!(
            envelope["attachments"],
            claim.integration_context.as_ref().unwrap().attachments
        );
        assert_eq!(
            envelope["tool_result"],
            claim
                .integration_context
                .as_ref()
                .unwrap()
                .tool_result
                .clone()
                .unwrap()
        );
        assert_eq!(
            envelope["external_user"],
            serde_json::to_value(
                claim
                    .integration_context
                    .as_ref()
                    .unwrap()
                    .external_user
                    .as_ref()
                    .unwrap()
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn persistent_pi_rpc_process_reports_failed_terminal_state_without_error_details() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pi-failure.pid");
        let pi_bin = write_fake_pi_wrapper(&temp, &pid_file, &["FAKE_PI_DISABLE_MODEL=1"]);
        let mut claim = test_claim();
        claim.run.initial_message = "fixture:fail".into();
        let run_env = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:1/v1"))
            .await
            .unwrap();
        let cancellation = Arc::new(EngineCancellation::default());
        let mut process = pi_driver::PersistentPiRpcProcess::start(
            pi_bin.to_str().unwrap(),
            &run_env,
            None,
            &pi_driver::pi_tool_allowlist(&claim.agent),
            Duration::from_secs(2),
            cancellation,
        )
        .unwrap();

        let result = process.execute(&claim, None).unwrap();
        assert_eq!(result.final_status, "failed");
        let serialized = serde_json::to_string(&result.events).unwrap();
        assert!(!serialized.contains("errorMessage"));
        assert!(result.events.iter().any(|event| {
            event.event_type == "status" && event.content.as_deref() == Some("failed")
        }));
        drop(process);
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn persistent_pi_rpc_process_rejects_malformed_json_and_reaps_child() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pi-malformed.pid");
        let pi_bin = write_fake_pi_wrapper(
            &temp,
            &pid_file,
            &[
                "FAKE_PI_DISABLE_MODEL=1",
                "FAKE_PI_MALFORMED_AFTER_PROMPT=1",
            ],
        );
        let claim = test_claim();
        let run_env = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:1/v1"))
            .await
            .unwrap();
        let cancellation = Arc::new(EngineCancellation::default());
        let mut process = pi_driver::PersistentPiRpcProcess::start(
            pi_bin.to_str().unwrap(),
            &run_env,
            None,
            &pi_driver::pi_tool_allowlist(&claim.agent),
            Duration::from_secs(2),
            cancellation,
        )
        .unwrap();

        let error = format!("{:#}", process.execute(&claim, None).unwrap_err());
        assert!(error.contains("parse Pi RPC JSON line"));
        drop(process);
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn persistent_pi_rpc_process_times_out_and_reaps_child() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pi-timeout.pid");
        let pi_bin = write_fake_pi_wrapper(&temp, &pid_file, &["FAKE_PI_DISABLE_MODEL=1"]);
        let mut claim = test_claim();
        claim.run.initial_message = "fixture:hold".into();
        let run_env = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:1/v1"))
            .await
            .unwrap();
        let cancellation = Arc::new(EngineCancellation::default());
        let mut process = pi_driver::PersistentPiRpcProcess::start(
            pi_bin.to_str().unwrap(),
            &run_env,
            None,
            &pi_driver::pi_tool_allowlist(&claim.agent),
            Duration::from_secs(1),
            cancellation,
        )
        .unwrap();

        let error = format!("{:#}", process.execute(&claim, None).unwrap_err());
        assert!(error.contains("Pi RPC process timed out"));
        drop(process);
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn persistent_pi_rpc_process_abort_finishes_as_interrupted() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pi-abort.pid");
        let pi_bin = write_fake_pi_wrapper(&temp, &pid_file, &["FAKE_PI_DISABLE_MODEL=1"]);
        let mut claim = test_claim();
        claim.run.initial_message = "fixture:hold".into();
        let run_env = prepare_run_env(temp.path(), &claim, Some("http://127.0.0.1:1/v1"))
            .await
            .unwrap();
        let cancellation = Arc::new(EngineCancellation::default());
        let process = pi_driver::PersistentPiRpcProcess::start(
            pi_bin.to_str().unwrap(),
            &run_env,
            None,
            &pi_driver::pi_tool_allowlist(&claim.agent),
            Duration::from_secs(2),
            cancellation,
        )
        .unwrap();
        let (command_tx, command_rx) = mpsc::channel();
        let (event_tx, mut event_rx) = engine_event_channel();
        let driver = std::thread::spawn(move || {
            let mut process = process;
            let mut deferred = VecDeque::new();
            process.execute_controlled(&claim, Some(event_tx), &command_rx, &mut deferred)
        });

        let native_turn_id = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let event = event_rx.recv().await.unwrap();
                if event.event_type == "turn_started" {
                    break event.payload["native_turn_id"].as_str().unwrap().to_owned();
                }
            }
        })
        .await
        .unwrap();
        let (response, outcome) = oneshot::channel();
        command_tx
            .send(SessionSupervisorCommand::Interrupt {
                expected_turn_id: native_turn_id,
                response,
            })
            .unwrap();
        assert_eq!(
            outcome.await.unwrap().unwrap(),
            SessionInterruptOutcome::Interrupted
        );
        let result = tokio::task::spawn_blocking(move || driver.join().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.final_status, "interrupted");
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn pi_session_supervisor_steers_once_and_reuses_its_process_for_next_run() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pi-supervisor-steer.pid");
        let request_log = temp.path().join("pi-supervisor-steer.jsonl");
        let request_log_env = format!("FAKE_PI_REQUEST_LOG={}", request_log.display());
        let pi_bin = write_fake_pi_wrapper(
            &temp,
            &pid_file,
            &["FAKE_PI_DISABLE_MODEL=1", &request_log_env],
        );
        let mut first = test_claim();
        let session_id = Uuid::new_v4();
        first.run.hub_session_id = Some(session_id);
        first.run.initial_message = "fixture:hold".into();
        let run_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let supervisor = SessionSupervisor::start_pi(
            session_id,
            1,
            pi_bin.display().to_string(),
            run_env,
            None,
            pi_driver::pi_tool_allowlist(&first.agent),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let (event_tx, mut event_rx) = engine_event_channel();
        let first_turn_id = first.run.id.to_string();
        let execution = tokio::spawn({
            let supervisor = Arc::clone(&supervisor);
            async move { supervisor.execute(first, Some(event_tx)).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let event = event_rx.recv().await.unwrap();
                if event.event_type == "turn_started" {
                    assert_eq!(event.payload["native_turn_id"], first_turn_id);
                    break;
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(
            supervisor
                .steer(
                    1,
                    first_turn_id.clone(),
                    Uuid::new_v4(),
                    vec!["fixture:release".into()],
                )
                .await
                .unwrap(),
            SessionSteerOutcome::Applied
        );
        assert_eq!(execution.await.unwrap().unwrap().final_status, "completed");

        let mut second = test_claim();
        second.run.id = Uuid::new_v4();
        second.run.hub_session_id = Some(session_id);
        second.execution_configuration.model_bindings[0].id = Uuid::new_v4();
        second.execution_configuration.model_bindings[0].run_id = second.run.id;
        second.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&second.execution_configuration).unwrap();
        prepare_run_env(temp.path(), &second, None).await.unwrap();
        assert_eq!(
            supervisor.execute(second, None).await.unwrap().final_status,
            "completed"
        );

        let requests = std::fs::read_to_string(&request_log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["type"] == "steer")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["type"] == "prompt")
                .count(),
            2
        );
        assert_eq!(
            std::fs::read_to_string(&pid_file).unwrap().lines().count(),
            1
        );

        supervisor.shutdown();
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn pi_session_supervisor_interrupts_without_erasing_session_and_continues() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pi-supervisor-interrupt.pid");
        let request_log = temp.path().join("pi-supervisor-interrupt.jsonl");
        let request_log_env = format!("FAKE_PI_REQUEST_LOG={}", request_log.display());
        let pi_bin = write_fake_pi_wrapper(
            &temp,
            &pid_file,
            &["FAKE_PI_DISABLE_MODEL=1", &request_log_env],
        );
        let mut first = test_claim();
        let session_id = Uuid::new_v4();
        first.run.hub_session_id = Some(session_id);
        first.run.initial_message = "fixture:hold".into();
        let run_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let supervisor = SessionSupervisor::start_pi(
            session_id,
            1,
            pi_bin.display().to_string(),
            run_env,
            None,
            pi_driver::pi_tool_allowlist(&first.agent),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let (event_tx, mut event_rx) = engine_event_channel();
        let first_turn_id = first.run.id.to_string();
        let execution = tokio::spawn({
            let supervisor = Arc::clone(&supervisor);
            async move { supervisor.execute(first, Some(event_tx)).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let event = event_rx.recv().await.unwrap();
                if event.event_type == "turn_started" {
                    break;
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(
            supervisor.interrupt(1, first_turn_id).await.unwrap(),
            SessionInterruptOutcome::Interrupted
        );
        assert_eq!(
            execution.await.unwrap().unwrap().final_status,
            "interrupted"
        );

        let mut second = test_claim();
        second.run.id = Uuid::new_v4();
        second.run.hub_session_id = Some(session_id);
        second.execution_configuration.model_bindings[0].id = Uuid::new_v4();
        second.execution_configuration.model_bindings[0].run_id = second.run.id;
        second.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&second.execution_configuration).unwrap();
        prepare_run_env(temp.path(), &second, None).await.unwrap();
        assert_eq!(
            supervisor.execute(second, None).await.unwrap().final_status,
            "completed"
        );

        let session_file = SessionPaths::for_session(temp.path(), session_id)
            .engine_state
            .join("sessions/fake-pi-session.jsonl");
        let session_events = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let events = std::fs::read_to_string(&session_file)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .collect::<Vec<_>>();
                if events.iter().any(|event| event["type"] == "aborted")
                    && events.iter().any(|event| event["type"] == "assistant")
                {
                    break events;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(session_events
            .iter()
            .any(|event| event["type"] == "aborted"));
        assert!(session_events
            .iter()
            .any(|event| event["type"] == "assistant"));
        let requests = std::fs::read_to_string(&request_log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["type"] == "abort")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["type"] == "prompt")
                .count(),
            2
        );
        assert_eq!(
            std::fs::read_to_string(&pid_file).unwrap().lines().count(),
            1
        );

        supervisor.shutdown();
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn pi_cold_restart_reopens_the_discovered_session_file() {
        let temp = tempfile::tempdir().unwrap();
        let first_pid_file = temp.path().join("pi-cold-first.pid");
        let first_bin = write_fake_pi_wrapper(&temp, &first_pid_file, &["FAKE_PI_DISABLE_MODEL=1"]);
        let first = test_claim();
        let run_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let mut first_process = pi_driver::PersistentPiRpcProcess::start(
            first_bin.to_str().unwrap(),
            &run_env,
            None,
            &pi_driver::pi_tool_allowlist(&first.agent),
            Duration::from_secs(2),
            Arc::new(EngineCancellation::default()),
        )
        .unwrap();
        assert_eq!(
            first_process.execute(&first, None).unwrap().final_status,
            "completed"
        );
        let native_session_id = first_process.native_session_id().to_owned();
        let first_session_file = first_process.session_file().to_path_buf();
        drop(first_process);
        assert_process_group_reaped_or_clean_up(&first_pid_file);

        let recovered_session_file =
            pi_driver::discover_session_file(&run_env.engine_state_root, &native_session_id)
                .unwrap();
        assert_eq!(recovered_session_file, first_session_file);

        let second_pid_file = temp.path().join("pi-cold-second.pid");
        let second_bin =
            write_fake_pi_wrapper(&temp, &second_pid_file, &["FAKE_PI_DISABLE_MODEL=1"]);
        let mut second = first.clone();
        second.run.id = Uuid::new_v4();
        second.execution_configuration.model_bindings[0].id = Uuid::new_v4();
        second.execution_configuration.model_bindings[0].run_id = second.run.id;
        second.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&second.execution_configuration).unwrap();
        let second_run_env = prepare_run_env(temp.path(), &second, None).await.unwrap();
        let mut recovered_process = pi_driver::PersistentPiRpcProcess::start(
            second_bin.to_str().unwrap(),
            &second_run_env,
            Some(&recovered_session_file),
            &pi_driver::pi_tool_allowlist(&second.agent),
            Duration::from_secs(2),
            Arc::new(EngineCancellation::default()),
        )
        .unwrap();
        assert_eq!(recovered_process.native_session_id(), native_session_id);
        assert_eq!(recovered_process.session_file(), recovered_session_file);
        assert_eq!(
            recovered_process
                .execute(&second, None)
                .unwrap()
                .final_status,
            "completed"
        );
        drop(recovered_process);
        assert_process_group_reaped_or_clean_up(&second_pid_file);
    }

    #[tokio::test]
    async fn persistent_pi_rpc_process_reloads_next_run_model_without_restarting() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pi-reload.pid");
        let request_log = temp.path().join("pi-reload-requests.jsonl");
        let request_log_env = format!("FAKE_PI_REQUEST_LOG={}", request_log.display());
        let pi_bin = write_fake_pi_wrapper(
            &temp,
            &pid_file,
            &["FAKE_PI_DISABLE_MODEL=1", &request_log_env],
        );
        let first = test_claim();
        let run_env = prepare_run_env(temp.path(), &first, Some("http://127.0.0.1:1/v1"))
            .await
            .unwrap();
        let cancellation = Arc::new(EngineCancellation::default());
        let mut process = pi_driver::PersistentPiRpcProcess::start(
            pi_bin.to_str().unwrap(),
            &run_env,
            None,
            &pi_driver::pi_tool_allowlist(&first.agent),
            Duration::from_secs(2),
            cancellation,
        )
        .unwrap();

        process.execute(&first, None).unwrap();
        let first_pid = std::fs::read_to_string(&pid_file).unwrap();

        let mut second = first.clone();
        second.run.id = Uuid::new_v4();
        let binding = second
            .execution_configuration
            .model_bindings
            .first_mut()
            .unwrap();
        binding.id = Uuid::new_v4();
        binding.run_id = second.run.id;
        binding.model_id = "gpt-second".into();
        second
            .execution_configuration
            .model_selection
            .as_mut()
            .unwrap()
            .model_id = "gpt-second".into();
        second.agent.model_selection.as_mut().unwrap().model_id = "gpt-second".into();
        second.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&second.execution_configuration).unwrap();
        prepare_run_env(temp.path(), &second, Some("http://127.0.0.1:1/v1"))
            .await
            .unwrap();

        let result = process.execute(&second, None).unwrap();
        assert_eq!(result.final_status, "completed");
        assert_eq!(std::fs::read_to_string(&pid_file).unwrap(), first_pid);
        let requests = std::fs::read_to_string(&request_log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["type"] == "reload_models")
                .count(),
            2
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["type"] == "set_model")
                .map(|request| request["modelId"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["gpt-main", "gpt-second"]
        );

        drop(process);
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_pi_standalone_reloads_two_run_bindings_on_one_session() {
        let Ok(pi_bin) = env::var("PI_STANDALONE_BIN") else {
            return;
        };
        let pi_bin = PathBuf::from(pi_bin).canonicalize().unwrap();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new().route(
            "/v1/responses",
            post({
                let observed = Arc::clone(&observed);
                move |headers: HeaderMap, body: Bytes| {
                    let observed = Arc::clone(&observed);
                    async move {
                        let request = serde_json::from_slice::<Value>(&body).unwrap();
                        observed.lock().unwrap().push((
                            headers
                                .get("x-agent-hub-model-binding-id")
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .to_owned(),
                            request["model"].as_str().unwrap().to_owned(),
                        ));
                        let response = json!({
                            "id": "resp_agent_hub_pi",
                            "object": "response",
                            "status": "completed",
                            "output": [],
                            "usage": {
                                "input_tokens": 5,
                                "output_tokens": 3,
                                "total_tokens": 8,
                                "input_tokens_details": { "cached_tokens": 0 }
                            }
                        });
                        let output_item = json!({
                            "type": "message",
                            "id": "msg_agent_hub_pi",
                            "role": "assistant",
                            "status": "completed",
                            "content": [{ "type": "output_text", "text": "Hello from real Pi" }]
                        });
                        let sse = [
                            json!({
                                "type": "response.output_item.added",
                                "output_index": 0,
                                "item": {
                                    "type": "message",
                                    "id": "msg_agent_hub_pi",
                                    "role": "assistant",
                                    "status": "in_progress",
                                    "content": []
                                }
                            }),
                            json!({
                                "type": "response.content_part.added",
                                "output_index": 0,
                                "content_index": 0,
                                "part": { "type": "output_text", "text": "" }
                            }),
                            json!({
                                "type": "response.output_text.delta",
                                "output_index": 0,
                                "content_index": 0,
                                "delta": "Hello from real Pi"
                            }),
                            json!({
                                "type": "response.output_item.done",
                                "output_index": 0,
                                "item": output_item
                            }),
                            json!({ "type": "response.completed", "response": response }),
                        ]
                        .into_iter()
                        .map(|event| format!("data: {event}\n\n"))
                        .collect::<String>();
                        ([(header::CONTENT_TYPE, "text/event-stream")], sse)
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model_addr = listener.local_addr().unwrap();
        let model_server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let temp = tempfile::tempdir().unwrap();
        let mut first = test_claim();
        first.run.initial_message = "first real Pi turn".into();
        let first_binding_id = first.execution_configuration.model_bindings[0].id;
        let model_base_url = format!("http://{model_addr}/v1");
        let run_env = prepare_run_env(temp.path(), &first, Some(&model_base_url))
            .await
            .unwrap();
        let mut process = pi_driver::PersistentPiRpcProcess::start(
            pi_bin.to_str().unwrap(),
            &run_env,
            None,
            &pi_driver::pi_tool_allowlist(&first.agent),
            Duration::from_secs(15),
            Arc::new(EngineCancellation::default()),
        )
        .unwrap();

        assert_eq!(
            process.execute(&first, None).unwrap().final_status,
            "completed"
        );

        let mut second = first.clone();
        second.run.id = Uuid::new_v4();
        second.run.initial_message = "second real Pi turn".into();
        let second_binding_id = Uuid::new_v4();
        let binding = second
            .execution_configuration
            .model_bindings
            .first_mut()
            .unwrap();
        binding.id = second_binding_id;
        binding.run_id = second.run.id;
        binding.model_id = "gpt-second".into();
        second
            .execution_configuration
            .model_selection
            .as_mut()
            .unwrap()
            .model_id = "gpt-second".into();
        second.agent.model_selection.as_mut().unwrap().model_id = "gpt-second".into();
        second.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&second.execution_configuration).unwrap();
        prepare_run_env(temp.path(), &second, Some(&model_base_url))
            .await
            .unwrap();

        assert_eq!(
            process.execute(&second, None).unwrap().final_status,
            "completed"
        );
        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                (first_binding_id.to_string(), "gpt-main".into()),
                (second_binding_id.to_string(), "gpt-second".into()),
            ]
        );

        drop(process);
        model_server.abort();
    }

    #[test]
    fn pi_tool_allowlist_never_grants_bash_to_read_only_or_offline_agents() {
        let mut agent = test_claim().agent;
        agent.sandbox_policy = json!({ "mode": "read-only", "network_access": true });
        assert_eq!(
            pi_driver::pi_tool_allowlist(&agent),
            ["read", "grep", "find", "ls"]
        );

        agent.sandbox_policy = json!({ "mode": "workspace-write", "network_access": false });
        assert_eq!(
            pi_driver::pi_tool_allowlist(&agent),
            ["read", "grep", "find", "ls", "edit", "write"]
        );

        agent.sandbox_policy = json!({ "mode": "workspace-write", "network_access": true });
        assert_eq!(
            pi_driver::pi_tool_allowlist(&agent),
            ["read", "grep", "find", "ls", "edit", "write", "bash"]
        );
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
        for entry in WalkDir::new(&run_env.engine_state_root) {
            let entry = entry.unwrap();
            if !entry.file_type().is_file() {
                continue;
            }
            let contents = stdfs::read(entry.path()).unwrap();
            let contents = String::from_utf8_lossy(&contents);
            assert!(!contents.contains(PROVIDER_URL));
            assert!(!contents.contains(PROVIDER_API_KEY));
        }
        let (events, _) = fake_engine_events(&claim);
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
        assert_eq!(request.engine_version, config.engine_version);
        assert_eq!(request.capabilities["mcp_allowlist"], false);
        assert_eq!(request.capabilities["subagents"], false);
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
    async fn local_model_proxy_requires_binding_and_preserves_query_safe_headers_and_body() {
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
        let binding_id = Uuid::new_v4();
        let proxy = start_model_proxy(
            &client,
            run_id,
            "scoped-model-token",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        proxy.acknowledge_turn(run_id).unwrap();
        let request_body = br#"{"model":"gpt-main","input":"keep bytes"}"#;
        let legacy_response = reqwest::Client::new()
            .post(format!("{}/responses", proxy.base_url))
            .header(
                "x-agent-hub-model-connection-id",
                Uuid::new_v4().to_string(),
            )
            .body(request_body.as_slice())
            .send()
            .await
            .unwrap();
        assert_eq!(legacy_response.status(), StatusCode::BAD_REQUEST);
        assert!(forwarded.lock().unwrap().is_none());

        let response = reqwest::Client::new()
            .post(format!(
                "{}/responses?include=usage&trace=a%2Fb",
                proxy.base_url
            ))
            .header("x-agent-hub-model-binding-id", binding_id.to_string())
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
            request.headers.get("x-agent-hub-model-binding-id").unwrap(),
            binding_id.to_string().as_str()
        );
        assert!(request
            .headers
            .get("x-agent-hub-model-connection-id")
            .is_none());
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
        let run_id = Uuid::new_v4();
        let binding_id = Uuid::new_v4();
        let proxy = start_model_proxy(
            &client,
            run_id,
            "scoped-model-token",
            Duration::from_millis(150),
        )
        .await
        .unwrap();
        proxy.acknowledge_turn(run_id).unwrap();
        let http = reqwest::Client::new();

        let response = tokio::time::timeout(
            Duration::from_millis(150),
            http.post(format!("{}/responses", proxy.base_url))
                .header("x-agent-hub-model-binding-id", binding_id.to_string())
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
        let run_id = Uuid::new_v4();
        let binding_id = Uuid::new_v4();
        let proxy = start_model_proxy(
            &client,
            run_id,
            "scoped-model-token",
            Duration::from_millis(75),
        )
        .await
        .unwrap();
        proxy.acknowledge_turn(run_id).unwrap();
        let mut response = reqwest::Client::new()
            .post(format!("{}/responses", proxy.base_url))
            .header("x-agent-hub-model-binding-id", binding_id.to_string())
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
                            headers
                                .get("x-agent-hub-model-binding-id")
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
        let first_binding_id = Uuid::new_v4();
        let second_binding_id = Uuid::new_v4();
        let proxy = start_model_proxy(
            &client,
            first_run_id,
            "first-model-token",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        proxy.acknowledge_turn(first_run_id).unwrap();
        let stable_base_url = proxy.base_url.clone();
        let http = reqwest::Client::new();

        assert_eq!(
            http.post(format!("{stable_base_url}/responses"))
                .header("x-agent-hub-model-binding-id", first_binding_id.to_string())
                .body("{}")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        proxy.activate_run(second_run_id, "second-model-token");
        proxy.acknowledge_turn(second_run_id).unwrap();
        assert_eq!(proxy.base_url, stable_base_url);
        assert_eq!(
            http.post(format!("{stable_base_url}/responses"))
                .header(
                    "x-agent-hub-model-binding-id",
                    second_binding_id.to_string()
                )
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
                (
                    "Bearer first-model-token".into(),
                    first_run_id.to_string(),
                    first_binding_id.to_string(),
                ),
                (
                    "Bearer second-model-token".into(),
                    second_run_id.to_string(),
                    second_binding_id.to_string(),
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
                    native_session_id: Some("native-session".into()),
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
    async fn session_manager_restarts_pi_for_changed_tools_and_resumes_native_session() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("pi-tool-policy.pid");
        let launch_log = temp.path().join("pi-tool-policy-launches.log");
        let pi_bin = write_tool_recording_fake_pi_wrapper(&temp, &pid_file, &launch_log);
        let runtime_id = Uuid::new_v4();
        let manager = SessionSupervisorManager::new(temp.path().to_path_buf(), runtime_id, 1);

        let mut first = test_claim();
        first.run.runtime_id = Some(runtime_id);
        let session_id = first.run.hub_session_id.unwrap();
        let first_metadata =
            session_supervisor_metadata_for_claim(runtime_id, &first, "test-engine").unwrap();
        let first_run_env = prepare_run_env(temp.path(), &first, None).await.unwrap();
        let first_tools = pi_driver::pi_tool_allowlist_for_claim(&first).unwrap();
        let first_supervisor = manager
            .ensure_pi(
                first_metadata.clone(),
                pi_bin.display().to_string(),
                first_run_env,
                first_tools,
                Duration::from_secs(2),
                None,
            )
            .await
            .unwrap();
        let first_result = first_supervisor.execute(first.clone(), None).await.unwrap();
        let native_session_id = first_result.native_session_id.unwrap();
        manager
            .update_native_session_id(session_id, 1, Some(&native_session_id))
            .await
            .unwrap();

        let mut second = first;
        second.run.id = Uuid::new_v4();
        second.execution_configuration.model_bindings[0].id = Uuid::new_v4();
        second.execution_configuration.model_bindings[0].run_id = second.run.id;
        second.agent.sandbox_policy = json!({ "mode": "read-only", "network_access": false });
        second.agent.tool_allowlist = vec!["read".into(), "grep".into()];
        second.execution_configuration.sandbox_policy = second.agent.sandbox_policy.clone();
        second.execution_configuration.tool_allowlist = second.agent.tool_allowlist.clone();
        second.expected_configuration_fingerprint =
            execution_configuration_fingerprint(&second.execution_configuration).unwrap();
        let second_run_env = prepare_run_env(temp.path(), &second, None).await.unwrap();
        let second_tools = pi_driver::pi_tool_allowlist_for_claim(&second).unwrap();
        let mut second_metadata = first_metadata;
        second_metadata.native_session_id = Some(native_session_id.clone());
        let second_supervisor = manager
            .ensure_pi(
                second_metadata,
                pi_bin.display().to_string(),
                second_run_env,
                second_tools,
                Duration::from_secs(2),
                None,
            )
            .await
            .unwrap();

        assert!(!Arc::ptr_eq(&first_supervisor, &second_supervisor));
        let second_result = second_supervisor.execute(second, None).await.unwrap();
        assert_eq!(
            second_result.native_session_id.as_deref(),
            Some(native_session_id.as_str())
        );
        let launches = std::fs::read_to_string(&launch_log).unwrap();
        let launches = launches.lines().collect::<Vec<_>>();
        assert_eq!(launches.len(), 2);
        assert!(launches[0].contains("--tools read,grep,find,ls,edit,write,bash"));
        assert!(launches[1].contains("--tools read,grep"));

        manager.shutdown();
        assert_process_group_reaped_or_clean_up(&pid_file);
    }

    #[tokio::test]
    async fn managed_session_waits_for_turn_ack_and_reuses_proxy_while_switching_run_auth() {
        let temp = tempfile::tempdir().unwrap();
        let pid_file = temp.path().join("managed-pi.pid");
        let request_log = temp.path().join("managed-pi-requests.jsonl");
        let request_log_env = format!("FAKE_PI_REQUEST_LOG={}", request_log.display());
        let pi_bin = write_fake_pi_wrapper(&temp, &pid_file, &[&request_log_env]);

        let forwarded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let acknowledged_turns = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/runtime/model-proxy/v1/{*path}",
                post({
                    let forwarded = Arc::clone(&forwarded);
                    let acknowledged_turns = Arc::clone(&acknowledged_turns);
                    move |headers: HeaderMap| {
                        let forwarded = Arc::clone(&forwarded);
                        let acknowledged_turns = Arc::clone(&acknowledged_turns);
                        async move {
                            let forwarded_count = forwarded.lock().unwrap().len();
                            if acknowledged_turns.load(Ordering::SeqCst) <= forwarded_count {
                                return AxumStatusCode::UNAUTHORIZED.into_response();
                            }
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
                                headers
                                    .get("x-agent-hub-model-binding-id")
                                    .unwrap()
                                    .to_str()
                                    .unwrap()
                                    .to_owned(),
                            ));
                            Json(json!({ "output_text": "done" })).into_response()
                        }
                    }
                }),
            )
            .route(
                "/api/runtime/runs/{run_id}/events",
                post({
                    let acknowledged_turns = Arc::clone(&acknowledged_turns);
                    move |Json(request): Json<Value>| {
                        let acknowledged_turns = Arc::clone(&acknowledged_turns);
                        async move {
                            if request
                                .pointer("/payload/event_type")
                                .and_then(Value::as_str)
                                == Some("turn_started")
                            {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                acknowledged_turns.fetch_add(1, Ordering::SeqCst);
                            }
                            AxumStatusCode::OK
                        }
                    }
                }),
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
        config.engine_bin = pi_bin.display().to_string();
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
        let first_proxy_base_url = manager
            .model_proxy(first.run.hub_session_id.unwrap())
            .unwrap()
            .base_url
            .clone();

        let mut second = first.clone();
        second.run.id = Uuid::new_v4();
        second.execution_configuration.model_bindings[0].id = Uuid::new_v4();
        second.execution_configuration.model_bindings[0].run_id = second.run.id;
        second.model_proxy_token = "second-run-token".into();
        manager.reserve_claim(&second).unwrap();
        execute_managed_run(&config, &client, Arc::clone(&manager), second.clone())
            .await
            .unwrap();
        let second_proxy_base_url = manager
            .model_proxy(second.run.hub_session_id.unwrap())
            .unwrap()
            .base_url
            .clone();

        assert_eq!(first_proxy_base_url, second_proxy_base_url);
        let requests = std::fs::read_to_string(&request_log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["type"] == "prompt")
                .count(),
            2
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["type"] == "reload_models")
                .count(),
            2
        );
        assert_eq!(
            std::fs::read_to_string(&pid_file).unwrap().lines().count(),
            1
        );
        let proxy_addr: SocketAddr = first_proxy_base_url
            .trim_start_matches("http://")
            .trim_end_matches("/v1")
            .parse()
            .unwrap();
        assert_eq!(
            *forwarded.lock().unwrap(),
            vec![
                (
                    "Bearer first-run-token".into(),
                    first.run.id.to_string(),
                    first.execution_configuration.model_bindings[0]
                        .id
                        .to_string(),
                ),
                (
                    "Bearer second-run-token".into(),
                    second.run.id.to_string(),
                    second.execution_configuration.model_bindings[0]
                        .id
                        .to_string(),
                ),
            ]
        );
        let metadata_path =
            SessionPaths::for_session(temp.path(), first.run.hub_session_id.unwrap())
                .supervisor
                .join(SESSION_SUPERVISOR_METADATA_FILE);
        let metadata: SessionSupervisorMetadata =
            serde_json::from_slice(&std::fs::read(metadata_path).unwrap()).unwrap();
        let native_session_id = "fake-pi-fake-pi-session";
        assert_eq!(
            metadata.native_session_id.as_deref(),
            Some(native_session_id)
        );
        manager.shutdown();
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
        .expect("manager shutdown must close the Session model proxy listener");
        let session_id = first.run.hub_session_id.unwrap();
        let recovery = plan_session_recovery(
            temp.path(),
            runtime_id,
            &[RuntimeOwnedSessionSnapshotDto {
                session_id,
                ownership_generation: 1,
                lifecycle_status: "online".into(),
                native_session_id: Some(native_session_id.into()),
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
    async fn idle_child_exit_is_reconciled_to_blocked_without_waiting_for_another_claim() {
        let temp = tempfile::tempdir().unwrap();
        let starts = temp.path().join("idle-crash-starts");
        let script = temp.path().join("idle-crashing-pi");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
echo start >> {}
session_dir=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --session-dir) session_dir="$2"; shift 2 ;;
    --mode|--tools) shift 2 ;;
    --no-extensions|--no-themes|--no-prompt-templates|--approve) shift ;;
    *) exit 64 ;;
  esac
done
mkdir -p "$session_dir"
session_file="$session_dir/idle-pi.jsonl"
printf '%s\n' '{{"type":"session","id":"idle-pi"}}' > "$session_file"
while IFS= read -r line; do
  command="$(printf '%s\n' "$line" | jq -r '.type')"
  request_id="$(printf '%s\n' "$line" | jq -r '.id')"
  case "$command" in
    get_state)
      jq -cn --arg id "$request_id" --arg file "$session_file" '{{type:"response",id:$id,command:"get_state",success:true,data:{{sessionFile:$file,sessionId:"idle-pi"}}}}'
      ;;
    reload_models|set_model|set_thinking_level)
      jq -cn --arg id "$request_id" --arg command "$command" '{{type:"response",id:$id,command:$command,success:true,data:null}}'
      ;;
    prompt)
      jq -cn --arg id "$request_id" '{{type:"response",id:$id,command:"prompt",success:true,data:null}}'
      jq -cn '{{type:"agent_start"}}'
      jq -cn '{{type:"turn_start"}}'
      jq -cn '{{type:"message_end",message:{{role:"assistant",content:[{{type:"text",text:"done"}}],stopReason:"stop"}}}}'
      jq -cn '{{type:"turn_end",message:{{role:"assistant",content:[{{type:"text",text:"done"}}],stopReason:"stop"}},toolResults:[]}}'
      jq -cn '{{type:"agent_end",messages:[],willRetry:false}}'
      jq -cn '{{type:"agent_settled"}}'
      sleep 0.1
      exit 72
      ;;
    *) exit 64 ;;
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
        config.engine_bin = script.display().to_string();
        config.hub_url = format!("http://{hub_addr}");
        config.engine_timeout = Duration::from_secs(5);
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
        config.engine_bin = temp.path().join("missing-pi").display().to_string();
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
            .expect_err("missing Pi binary must fail Session startup");

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
        let script = temp.path().join("worker-fake-pi");
        std::fs::write(
            &script,
            format!(
                r#"#!/bin/sh
session_dir=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --session-dir) session_dir="$2"; shift 2 ;;
    --mode|--tools) shift 2 ;;
    --no-extensions|--no-themes|--no-prompt-templates|--approve) shift ;;
    *) exit 64 ;;
  esac
done
mkdir -p "$session_dir"
session_id="worker-pi-$(basename "$(dirname "$(dirname "$session_dir")")")"
session_file="$session_dir/session.jsonl"
printf '{{"type":"session","id":"%s"}}\n' "$session_id" > "$session_file"
while IFS= read -r line; do
  command="$(printf '%s\n' "$line" | jq -r '.type')"
  request_id="$(printf '%s\n' "$line" | jq -r '.id')"
  case "$command" in
    get_state)
      jq -cn --arg id "$request_id" --arg file "$session_file" --arg session "$session_id" '{{type:"response",id:$id,command:"get_state",success:true,data:{{sessionFile:$file,sessionId:$session}}}}'
      ;;
    reload_models|set_model|set_thinking_level)
      jq -cn --arg id "$request_id" --arg command "$command" '{{type:"response",id:$id,command:$command,success:true,data:null}}'
      ;;
    prompt)
      jq -cn --arg id "$request_id" '{{type:"response",id:$id,command:"prompt",success:true,data:null}}'
      jq -cn '{{type:"agent_start"}}'
      jq -cn '{{type:"turn_start"}}'
      echo start >> "$PWD/turns"
      touch "$PWD/entered"
      while [ ! -f {} ]; do sleep 0.01; done
      echo done >> "$PWD/turns"
      jq -cn '{{type:"message_end",message:{{role:"assistant",content:[{{type:"text",text:"done"}}],stopReason:"stop"}}}}'
      jq -cn '{{type:"turn_end",message:{{role:"assistant",content:[{{type:"text",text:"done"}}],stopReason:"stop"}},toolResults:[]}}'
      jq -cn '{{type:"agent_end",messages:[],willRetry:false}}'
      jq -cn '{{type:"agent_settled"}}'
      ;;
    *) exit 64 ;;
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
        config.engine_bin = script.display().to_string();
        config.hub_url = format!("http://{hub_addr}");
        config.engine_timeout = Duration::from_secs(3);
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
            pi_agent_directory(&env.engine_state_root)
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
        let agent_dir = pi_agent_directory(&env.engine_state_root);
        let first_dir = agent_dir.join("skills").join(skill_directory_name("审查"));
        let second_dir = agent_dir.join("skills").join(skill_directory_name("评审"));

        assert_ne!(first_dir, second_dir);
        assert!(first_dir.join("SKILL.md").exists());
        assert!(second_dir.join("SKILL.md").exists());
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

        assert!(pi_agent_directory(&env.engine_state_root)
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
            pi_agent_directory(&env.engine_state_root)
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
            native_session_id: "thread-parent".into(),
            work_dir_ref: Some(parent.display().to_string()),
        });

        let env = prepare_run_env(temp.path(), &claim, None).await.unwrap();

        assert!(fs::metadata(env.workdir.join("nested/state.txt"))
            .await
            .is_err());
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

    fn write_fake_pi_wrapper(
        temp: &tempfile::TempDir,
        pid_file: &Path,
        environment: &[&str],
    ) -> PathBuf {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/fake-pi-rpc.sh")
            .canonicalize()
            .unwrap();
        let script = temp
            .path()
            .join(format!("fake-pi-{}", Uuid::new_v4().simple()));
        let exports = environment
            .iter()
            .map(|assignment| format!("export {assignment}\n"))
            .collect::<String>();
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\necho $$ > {}\n{}exec {} \"$@\"\n",
                shell_single_quote(pid_file),
                exports,
                shell_single_quote(&fixture)
            ),
        )
        .unwrap();
        make_executable(&script);
        script
    }

    fn write_tool_recording_fake_pi_wrapper(
        temp: &tempfile::TempDir,
        pid_file: &Path,
        launch_log: &Path,
    ) -> PathBuf {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/fake-pi-rpc.sh")
            .canonicalize()
            .unwrap();
        let fixture_source = std::fs::read_to_string(&fixture).unwrap();
        let fixture_body = fixture_source
            .split_once('\n')
            .map(|(_, body)| body)
            .unwrap_or(fixture_source.as_str());
        let script = temp
            .path()
            .join(format!("fake-pi-tools-{}", Uuid::new_v4().simple()));
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\necho $$ > {}\nprintf '%s\\n' \"$*\" >> {}\nexport FAKE_PI_DISABLE_MODEL=1\n{}",
                shell_single_quote(pid_file),
                shell_single_quote(launch_log),
                fixture_body
            ),
        )
        .unwrap();
        make_executable(&script);
        script
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
                if !process_group_has_live_members(pid) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            if !process_group_has_live_members(pid) {
                return;
            }
            unsafe {
                libc::kill(-pid, libc::SIGKILL);
            }
            panic!("Execution Engine process group {pid} survived runtime cancellation");
        }
    }

    #[cfg(unix)]
    fn process_group_exists(process_group_id: i32) -> bool {
        if unsafe { libc::kill(-process_group_id, 0) } == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }

    #[cfg(unix)]
    fn process_group_has_live_members(process_group_id: i32) -> bool {
        #[cfg(target_os = "linux")]
        {
            // A killed descendant can remain a zombie until the container's PID 1 reaps it.
            process_group_members(process_group_id)
                .map(|members| {
                    members
                        .into_iter()
                        .any(|(_, state)| !matches!(state, 'Z' | 'X'))
                })
                .unwrap_or_else(|_| process_group_exists(process_group_id))
        }
        #[cfg(not(target_os = "linux"))]
        {
            process_group_exists(process_group_id)
        }
    }

    #[cfg(target_os = "linux")]
    fn process_group_members(process_group_id: i32) -> std::io::Result<Vec<(i32, char)>> {
        let members = std::fs::read_dir("/proc")?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let pid = entry.file_name().to_string_lossy().parse::<i32>().ok()?;
                let stat = std::fs::read_to_string(entry.path().join("stat")).ok()?;
                let (_, fields) = stat.rsplit_once(") ")?;
                let mut fields = fields.split_whitespace();
                let state = fields.next()?.chars().next()?;
                fields.next()?;
                let group_id = fields.next()?.parse::<i32>().ok()?;
                (group_id == process_group_id).then_some((pid, state))
            })
            .collect::<Vec<_>>();
        Ok(members)
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
        let run_id = Uuid::new_v4();
        let model_connection_id = Uuid::from_u128(0x201);
        let model_binding_id = Uuid::from_u128(0x101);
        let model_selection = ModelSelectionDto {
            connection_id: model_connection_id,
            model_id: "gpt-main".into(),
        };
        let model_settings = AgentModelSettings::default();
        let execution_configuration = AgentExecutionConfigurationDto {
            revision: 1,
            instructions: "Be concise".into(),
            model_selection: Some(model_selection.clone()),
            model_settings: model_settings.clone(),
            subagents: Vec::new(),
            model_bindings: vec![RunModelBindingDto {
                id: model_binding_id,
                run_id,
                binding_key: "main".into(),
                model_connection_id,
                connection_name_snapshot: "Main model".into(),
                connection_scope_snapshot: ModelConnectionScope::Global,
                model_id: "gpt-main".into(),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                model_settings: model_settings.clone(),
            }],
            model_policy: json!({ "provider": "hub-proxy" }),
            sandbox_policy: json!({ "mode": "workspace-write", "network_access": true }),
            skills: vec![test_execution_skill(
                "repo-review",
                "repo-review",
                "Check the diff.",
            )],
            mcp_allowlist: json!([{ "name": "filesystem", "command": "fs" }]),
            tool_allowlist: default_agent_tool_allowlist(),
        };
        let expected_configuration_fingerprint =
            execution_configuration_fingerprint(&execution_configuration).unwrap();
        ClaimRunResponse {
            run: RunDto {
                id: run_id,
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
                native_session_id: None,
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
                model_selection: Some(model_selection),
                model_settings,
                subagents: Vec::new(),
                model_policy: json!({ "provider": "hub-proxy" }),
                sandbox_policy: json!({ "mode": "workspace-write", "network_access": true }),
                managed_skill_ids: Vec::new(),
                mcp_allowlist: json!([{ "name": "filesystem", "command": "fs" }]),
                tool_allowlist: default_agent_tool_allowlist(),
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
            engine_driver: "pi".into(),
            engine_bin: "pi".into(),
            engine_version: "test-engine".into(),
            engine_timeout: Duration::from_secs(1),
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
