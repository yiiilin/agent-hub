use std::path::{Path, PathBuf};

use super::RunEnv;

pub(super) const SKILL_EXEC_EXTENSION_SOURCE: &str = r#"import { createConnection } from "node:net";

const socketPath = process.env.AGENT_HUB_SKILL_EXEC_SOCKET;
const token = process.env.AGENT_HUB_SKILL_EXEC_TOKEN;
if (!socketPath || !token) {
  throw new Error("Agent Hub Skill execution broker is not configured");
}

function callBroker(params, signal) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let buffer = "";
    const socket = createConnection(socketPath);
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      signal?.removeEventListener("abort", onAbort);
      socket.destroy();
      callback(value);
    };
    const onAbort = () => finish(reject, new Error("Skill execution aborted"));
    if (signal?.aborted) {
      onAbort();
      return;
    }
    signal?.addEventListener("abort", onAbort, { once: true });
    socket.setEncoding("utf8");
    socket.on("connect", () => {
      socket.write(`${JSON.stringify({ ...params, token })}\n`);
    });
    socket.on("data", (chunk) => {
      buffer += chunk;
      if (buffer.length > 2200000) {
        finish(reject, new Error("Skill execution response exceeded its limit"));
        return;
      }
      const newline = buffer.indexOf("\n");
      if (newline < 0) return;
      try {
        finish(resolve, JSON.parse(buffer.slice(0, newline)));
      } catch (error) {
        finish(reject, error);
      }
    });
    socket.on("error", (error) => finish(reject, error));
    socket.on("close", () => {
      if (!settled) finish(reject, new Error("Skill execution broker disconnected"));
    });
  });
}

export default function registerAgentHubSkillExec(pi) {
  pi.registerTool({
    name: "skill_exec",
    label: "Skill Exec",
    description: "Run one executable from an enabled Skill package without opening a general shell.",
    promptSnippet: "skill_exec(skill, program, args?, stdin?, timeout_ms?): run an enabled Skill client.",
    executionMode: "sequential",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["skill", "program"],
      properties: {
        skill: { type: "string", description: "Exact enabled Skill name" },
        program: { type: "string", description: "Exact package path under bin/, for example bin/client" },
        args: { type: "array", items: { type: "string" }, maxItems: 128 },
        stdin: { type: "string", description: "Optional UTF-8 standard input" },
        timeout_ms: { type: "integer", minimum: 1, maximum: 300000 },
      },
    },
    async execute(_toolCallId, params, signal) {
      const response = await callBroker(params, signal);
      if (!response.ok) throw new Error(response.error || "Skill execution failed");
      const lines = [];
      if (response.exit_code !== null) lines.push(`exit_code: ${response.exit_code}`);
      if (response.timed_out) lines.push("timed_out: true");
      if (response.output_limit_exceeded) lines.push("output_limit_exceeded: true");
      if (response.stdout) lines.push(`stdout:\n${response.stdout}`);
      if (response.stderr) lines.push(`stderr:\n${response.stderr}`);
      if (lines.length === 0) lines.push("Skill client completed without output.");
      return {
        content: [{ type: "text", text: lines.join("\n") }],
        details: response,
      };
    },
  });
}
"#;

#[cfg(not(target_os = "linux"))]
pub(super) struct SkillExecBroker;

#[cfg(not(target_os = "linux"))]
impl SkillExecBroker {
    pub(super) fn start(
        _run_env: &RunEnv,
        _tools: &[String],
        _hub_url: &str,
        _maintenance_token_file: Option<&Path>,
    ) -> anyhow::Result<Self> {
        anyhow::bail!("Skill execution requires Linux Landlock isolation")
    }

    pub(super) fn socket_path(&self) -> &Path {
        unreachable!("Skill execution is unavailable on this platform")
    }

    pub(super) fn token(&self) -> &str {
        unreachable!("Skill execution is unavailable on this platform")
    }
}

#[cfg(target_os = "linux")]
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    net::Shutdown,
    os::{
        fd::AsRawFd,
        unix::{
            fs::{MetadataExt, PermissionsExt},
            net::{UnixListener, UnixStream},
            process::CommandExt,
        },
    },
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use anyhow::Context;
#[cfg(target_os = "linux")]
use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
use super::{
    add_landlock_path_rule, create_landlock_ruleset, restrict_landlock_child,
    terminate_child_process_tree, workspace_landlock_access, PiFilesystemSandbox,
    LANDLOCK_ACCESS_FS_EXECUTE, LANDLOCK_ACCESS_FS_READ_DIR, LANDLOCK_ACCESS_FS_READ_FILE,
    LANDLOCK_DIRECTORY_LIST, LANDLOCK_FILE_READ, LANDLOCK_FILE_READ_ONLY, LANDLOCK_FILE_READ_WRITE,
    LANDLOCK_RUNTIME_WRITE, PI_PROCESS_PATH,
};

#[cfg(target_os = "linux")]
const SKILL_EXEC_DIRECTORY: &str = "skill-exec";
#[cfg(target_os = "linux")]
const SKILL_EXEC_CATALOG_FILE: &str = "catalog.json";
#[cfg(target_os = "linux")]
const SKILL_EXEC_PACKAGES_DIRECTORY: &str = "packages";
#[cfg(target_os = "linux")]
const SKILL_EXEC_TEMP_DIRECTORY: &str = "tmp";
#[cfg(target_os = "linux")]
const SKILL_EXEC_SOCKET_FILE: &str = "skill-exec.sock";
#[cfg(target_os = "linux")]
const MAX_CATALOG_BYTES: u64 = 1024 * 1024;
#[cfg(target_os = "linux")]
const MAX_REQUEST_BYTES: usize = 1024 * 1024 + 128 * 1024;
#[cfg(target_os = "linux")]
const MAX_ARGUMENTS: usize = 128;
#[cfg(target_os = "linux")]
const MAX_ARGUMENT_BYTES: usize = 16 * 1024;
#[cfg(target_os = "linux")]
const MAX_TOTAL_ARGUMENT_BYTES: usize = 128 * 1024;
#[cfg(target_os = "linux")]
const MAX_STDIN_BYTES: usize = 1024 * 1024;
#[cfg(target_os = "linux")]
const MAX_OUTPUT_BYTES: usize = 1024 * 1024;
#[cfg(target_os = "linux")]
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(target_os = "linux")]
const MAX_TIMEOUT: Duration = Duration::from_secs(300);
#[cfg(target_os = "linux")]
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(target_os = "linux")]
const LANDLOCK_DIRECTORY_READ: u64 = LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_READ_DIR;
pub(super) const AGENT_HUB_MAINTENANCE_SKILL_NAME: &str = "agent-hub-maintenance";

#[cfg(target_os = "linux")]
#[derive(Debug, Deserialize)]
struct SkillExecCatalog {
    version: u32,
    skills: Vec<SkillExecCatalogSkill>,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Deserialize)]
struct SkillExecCatalogSkill {
    name: String,
    package_id: String,
    package_root: PathBuf,
    executables: Vec<String>,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SkillExecRequest {
    token: String,
    skill: String,
    program: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    stdin: String,
    timeout_ms: Option<u64>,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Deserialize, Serialize)]
struct SkillExecResponse {
    ok: bool,
    error: Option<String>,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    timed_out: bool,
    output_limit_exceeded: bool,
    elapsed_ms: u64,
}

#[cfg(target_os = "linux")]
impl SkillExecResponse {
    fn error(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(error.into()),
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
            output_limit_exceeded: false,
            elapsed_ms: 0,
        }
    }
}

#[cfg(target_os = "linux")]
struct SkillExecOutput {
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    timed_out: bool,
    output_limit_exceeded: bool,
    elapsed: Duration,
}

#[cfg(target_os = "linux")]
struct SkillExecBrokerContext {
    catalog_path: PathBuf,
    packages_root: PathBuf,
    temp_root: PathBuf,
    workdir: PathBuf,
    tools: Vec<String>,
    token: String,
    stop: Arc<AtomicBool>,
    hub_url: Option<String>,
    maintenance_token_file: Option<PathBuf>,
}

#[cfg(target_os = "linux")]
struct SkillExecExecutionContext<'a> {
    packages_root: &'a Path,
    temp_root: &'a Path,
    workdir: &'a Path,
    tools: &'a [String],
    stop: &'a AtomicBool,
    disconnected: &'a AtomicBool,
    hub_url: Option<&'a str>,
    maintenance_token_file: Option<&'a Path>,
}

#[cfg(target_os = "linux")]
pub(super) struct SkillExecBroker {
    _socket_dir: tempfile::TempDir,
    socket_path: PathBuf,
    token: String,
    stop: Arc<AtomicBool>,
    actor: Option<thread::JoinHandle<()>>,
}

#[cfg(target_os = "linux")]
impl SkillExecBroker {
    pub(super) fn start(
        run_env: &RunEnv,
        tools: &[String],
        hub_url: &str,
        maintenance_token_file: Option<&Path>,
    ) -> anyhow::Result<Self> {
        let root = run_env.engine_state_root.join(SKILL_EXEC_DIRECTORY);
        let catalog_path = root.join(SKILL_EXEC_CATALOG_FILE);
        let packages_root = root.join(SKILL_EXEC_PACKAGES_DIRECTORY);
        let temp_root = root.join(SKILL_EXEC_TEMP_DIRECTORY);
        super::prepare_private_directory(&root, "Skill execution directory")?;
        super::prepare_private_directory(&packages_root, "Skill package execution directory")?;
        super::prepare_private_directory(&temp_root, "Skill execution temporary directory")?;
        let catalog = load_catalog(&catalog_path, &packages_root)
            .context("validate Skill execution catalog")?;
        let management_enabled = catalog
            .skills
            .iter()
            .any(|skill| skill.name == AGENT_HUB_MAINTENANCE_SKILL_NAME);

        // Session roots can exceed Linux's 107-byte Unix socket path limit.
        let socket_dir = tempfile::Builder::new()
            .prefix("ah-sx-")
            .tempdir_in("/tmp")
            .context("create private Skill execution socket directory")?;
        let socket_path = socket_dir.path().join(SKILL_EXEC_SOCKET_FILE);
        remove_socket_if_present(&socket_path)?;
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("bind Skill execution socket {}", socket_path.display()))?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
            .context("protect Skill execution socket")?;
        listener
            .set_nonblocking(true)
            .context("configure Skill execution socket")?;

        let token = uuid::Uuid::new_v4().simple().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let context = SkillExecBrokerContext {
            catalog_path,
            packages_root,
            temp_root,
            workdir: run_env.workdir.clone(),
            tools: tools.to_vec(),
            token: token.clone(),
            stop: Arc::clone(&stop),
            hub_url: management_enabled
                .then(|| hub_url.to_owned())
                .filter(|value| !value.is_empty()),
            maintenance_token_file: management_enabled
                .then(|| maintenance_token_file.map(Path::to_path_buf))
                .flatten(),
        };
        let actor = thread::spawn(move || run_broker(listener, &context));
        Ok(Self {
            _socket_dir: socket_dir,
            socket_path,
            token,
            stop,
            actor: Some(actor),
        })
    }

    pub(super) fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub(super) fn token(&self) -> &str {
        &self.token
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(actor) = self.actor.take() {
            let _ = actor.join();
        }
        let _ = remove_socket_if_present(&self.socket_path);
    }
}

#[cfg(target_os = "linux")]
impl Drop for SkillExecBroker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(target_os = "linux")]
fn run_broker(listener: UnixListener, context: &SkillExecBrokerContext) {
    while !context.stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => handle_connection(stream, context),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(_) => break,
        }
    }
}

#[cfg(target_os = "linux")]
fn handle_connection(mut stream: UnixStream, context: &SkillExecBrokerContext) {
    let response = (|| -> anyhow::Result<SkillExecResponse> {
        let request = read_request(&mut stream, &context.stop)?;
        anyhow::ensure!(
            constant_time_eq(request.token.as_bytes(), context.token.as_bytes()),
            "invalid Skill execution token"
        );
        validate_request(&request)?;
        let catalog = load_catalog(&context.catalog_path, &context.packages_root)?;
        let program = resolve_program(
            &catalog,
            &context.packages_root,
            &request.skill,
            &request.program,
        )?;

        let disconnected = Arc::new(AtomicBool::new(false));
        let monitor_done = Arc::new(AtomicBool::new(false));
        let monitor = monitor_disconnect(
            stream.try_clone().context("clone Skill execution socket")?,
            Arc::clone(&disconnected),
            Arc::clone(&monitor_done),
        );
        let execution_context = SkillExecExecutionContext {
            packages_root: &context.packages_root,
            temp_root: &context.temp_root,
            workdir: &context.workdir,
            tools: &context.tools,
            stop: &context.stop,
            disconnected: &disconnected,
            hub_url: context.hub_url.as_deref(),
            maintenance_token_file: context.maintenance_token_file.as_deref(),
        };
        let execution = execute_program(&program, &request, &execution_context);
        monitor_done.store(true, Ordering::Release);
        let _ = monitor.join();
        let output = execution?;
        Ok(SkillExecResponse {
            ok: true,
            error: None,
            exit_code: output.exit_code,
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            timed_out: output.timed_out,
            output_limit_exceeded: output.output_limit_exceeded,
            elapsed_ms: u64::try_from(output.elapsed.as_millis()).unwrap_or(u64::MAX),
        })
    })()
    .unwrap_or_else(|error| SkillExecResponse::error(error.to_string()));

    if let Ok(encoded) = serde_json::to_vec(&response) {
        let _ = stream.write_all(&encoded);
        let _ = stream.write_all(b"\n");
        let _ = stream.flush();
    }
    let _ = stream.shutdown(Shutdown::Both);
}

#[cfg(target_os = "linux")]
fn read_request(stream: &mut UnixStream, stop: &AtomicBool) -> anyhow::Result<SkillExecRequest> {
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .context("configure Skill execution request timeout")?;
    let started = Instant::now();
    let mut request = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        anyhow::ensure!(
            !stop.load(Ordering::Acquire),
            "Skill execution broker stopped"
        );
        anyhow::ensure!(
            started.elapsed() <= REQUEST_READ_TIMEOUT,
            "Skill execution request timed out"
        );
        match stream.read(&mut chunk) {
            Ok(0) => anyhow::bail!("Skill execution request disconnected"),
            Ok(read) => {
                let bytes = &chunk[..read];
                if let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
                    request.extend_from_slice(&bytes[..newline]);
                    anyhow::ensure!(
                        bytes[newline + 1..].iter().all(u8::is_ascii_whitespace),
                        "Skill execution request contains trailing data"
                    );
                    break;
                }
                request.extend_from_slice(bytes);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error).context("read Skill execution request"),
        }
        anyhow::ensure!(
            request.len() <= MAX_REQUEST_BYTES,
            "Skill execution request exceeded its limit"
        );
    }
    anyhow::ensure!(
        request.len() <= MAX_REQUEST_BYTES,
        "Skill execution request exceeded its limit"
    );
    serde_json::from_slice(&request).context("parse Skill execution request")
}

#[cfg(target_os = "linux")]
fn validate_request(request: &SkillExecRequest) -> anyhow::Result<()> {
    anyhow::ensure!(
        !request.skill.trim().is_empty() && request.skill.len() <= 256,
        "Skill execution Skill name is invalid"
    );
    validate_executable_path(&request.program)?;
    anyhow::ensure!(
        request.args.len() <= MAX_ARGUMENTS,
        "Skill execution has too many arguments"
    );
    let mut total = 0_usize;
    for argument in &request.args {
        anyhow::ensure!(
            !argument.as_bytes().contains(&0) && argument.len() <= MAX_ARGUMENT_BYTES,
            "Skill execution argument is invalid"
        );
        total = total
            .checked_add(argument.len())
            .context("Skill execution argument size overflow")?;
    }
    anyhow::ensure!(
        total <= MAX_TOTAL_ARGUMENT_BYTES,
        "Skill execution arguments exceeded their limit"
    );
    anyhow::ensure!(
        request.stdin.len() <= MAX_STDIN_BYTES,
        "Skill execution stdin exceeded its limit"
    );
    let timeout = request
        .timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_TIMEOUT);
    anyhow::ensure!(
        !timeout.is_zero() && timeout <= MAX_TIMEOUT,
        "Skill execution timeout is invalid"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn load_catalog(catalog_path: &Path, packages_root: &Path) -> anyhow::Result<SkillExecCatalog> {
    let metadata = fs::symlink_metadata(catalog_path)
        .with_context(|| format!("inspect Skill execution catalog {}", catalog_path.display()))?;
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "Skill execution catalog must be a regular file"
    );
    anyhow::ensure!(
        metadata.len() <= MAX_CATALOG_BYTES,
        "Skill execution catalog exceeded its limit"
    );
    anyhow::ensure!(
        metadata.mode() & 0o077 == 0,
        "Skill execution catalog permissions are too broad"
    );
    let catalog: SkillExecCatalog =
        serde_json::from_slice(&fs::read(catalog_path).context("read Skill execution catalog")?)
            .context("parse Skill execution catalog")?;
    anyhow::ensure!(
        catalog.version == 1,
        "unsupported Skill execution catalog version"
    );
    let canonical_packages =
        fs::canonicalize(packages_root).context("resolve Skill package execution directory")?;
    let mut names = BTreeSet::new();
    for skill in &catalog.skills {
        anyhow::ensure!(
            !skill.name.trim().is_empty() && names.insert(skill.name.as_str()),
            "Skill execution catalog contains an invalid or duplicate Skill name"
        );
        uuid::Uuid::parse_str(&skill.package_id)
            .context("Skill execution catalog contains an invalid package id")?;
        let metadata = fs::symlink_metadata(&skill.package_root)
            .context("inspect Skill execution package root")?;
        anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "Skill execution package root must be a regular directory"
        );
        let canonical_root = fs::canonicalize(&skill.package_root)
            .context("resolve Skill execution package root")?;
        anyhow::ensure!(
            canonical_root.parent() == Some(canonical_packages.as_path()),
            "Skill execution package root is outside the current Session"
        );
        let mut executable_paths = BTreeSet::new();
        for executable in &skill.executables {
            validate_executable_path(executable)?;
            anyhow::ensure!(
                executable_paths.insert(executable.as_str()),
                "Skill execution catalog contains a duplicate executable"
            );
            validated_program_path(&canonical_root, executable)?;
        }
    }
    Ok(catalog)
}

#[cfg(target_os = "linux")]
fn resolve_program(
    catalog: &SkillExecCatalog,
    packages_root: &Path,
    skill_name: &str,
    executable: &str,
) -> anyhow::Result<PathBuf> {
    let skill = catalog
        .skills
        .iter()
        .find(|skill| skill.name == skill_name)
        .context("Skill is not enabled for this Session")?;
    anyhow::ensure!(
        skill.executables.iter().any(|path| path == executable),
        "program is not executable for this Skill"
    );
    let canonical_packages = fs::canonicalize(packages_root)?;
    let canonical_root = fs::canonicalize(&skill.package_root)?;
    anyhow::ensure!(
        canonical_root.parent() == Some(canonical_packages.as_path()),
        "Skill execution package root is outside the current Session"
    );
    validated_program_path(&canonical_root, executable)
}

#[cfg(target_os = "linux")]
fn validate_executable_path(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty() && value.len() <= 4096 && !value.contains('\\'),
        "Skill executable path is invalid"
    );
    let components = Path::new(value).components().collect::<Vec<_>>();
    anyhow::ensure!(
        components.len() >= 2
            && components.first() == Some(&std::path::Component::Normal("bin".as_ref()))
            && components
                .iter()
                .all(|component| matches!(component, std::path::Component::Normal(_))),
        "Skill executable path must be a relative path below bin/"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn validated_program_path(package_root: &Path, executable: &str) -> anyhow::Result<PathBuf> {
    let mut current = package_root.to_path_buf();
    for component in Path::new(executable).components() {
        let std::path::Component::Normal(component) = component else {
            anyhow::bail!("Skill executable path is invalid");
        };
        current.push(component);
        let metadata = fs::symlink_metadata(&current).context("inspect Skill executable path")?;
        anyhow::ensure!(
            !metadata.file_type().is_symlink(),
            "Skill executable path cannot contain symbolic links"
        );
    }
    let metadata = fs::metadata(&current).context("inspect Skill executable")?;
    anyhow::ensure!(
        metadata.is_file(),
        "Skill executable must be a regular file"
    );
    anyhow::ensure!(
        metadata.mode() & 0o111 != 0,
        "Skill executable does not have execute permission"
    );
    let canonical = fs::canonicalize(&current).context("resolve Skill executable")?;
    anyhow::ensure!(
        canonical.strip_prefix(package_root).ok() == Some(Path::new(executable)),
        "Skill executable resolved outside its package"
    );
    Ok(canonical)
}

#[cfg(target_os = "linux")]
fn execute_program(
    program: &Path,
    request: &SkillExecRequest,
    context: &SkillExecExecutionContext<'_>,
) -> anyhow::Result<SkillExecOutput> {
    let packages_root = fs::canonicalize(context.packages_root)
        .context("resolve Skill package execution directory")?;
    let package_root = program
        .ancestors()
        .find(|ancestor| ancestor.parent() == Some(packages_root.as_path()))
        .context("Skill executable package root is invalid")?;
    let call_temp = context
        .temp_root
        .join(uuid::Uuid::new_v4().simple().to_string());
    fs::create_dir(&call_temp).context("create Skill execution temporary directory")?;
    fs::set_permissions(&call_temp, fs::Permissions::from_mode(0o700))
        .context("protect Skill execution temporary directory")?;
    let result = execute_program_in_temp(program, package_root, &call_temp, request, context);
    let cleanup =
        fs::remove_dir_all(&call_temp).context("remove Skill execution temporary directory");
    match (result, cleanup) {
        (Ok(output), Ok(())) => Ok(output),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

#[cfg(target_os = "linux")]
fn execute_program_in_temp(
    program: &Path,
    package_root: &Path,
    call_temp: &Path,
    request: &SkillExecRequest,
    context: &SkillExecExecutionContext<'_>,
) -> anyhow::Result<SkillExecOutput> {
    let launch = executable_launch(program)?;
    let sandbox = SkillExecFilesystemSandbox::prepare(
        program,
        package_root,
        launch.interpreter.as_deref(),
        call_temp,
        context.workdir,
        context.tools,
        context.maintenance_token_file,
    )?;
    let mut command = Command::new(launch.interpreter.as_deref().unwrap_or(program));
    command.env_clear();
    if launch.interpreter.is_some() {
        command.arg(program);
    }
    command
        .args(&request.args)
        .current_dir(context.workdir)
        .env("HOME", call_temp)
        .env("TMPDIR", call_temp)
        .env("TMP", call_temp)
        .env("TEMP", call_temp)
        .env("PATH", PI_PROCESS_PATH)
        .env("AGENT_HUB_SKILL_ROOT", package_root)
        .env("AGENT_HUB_SKILL_TMPDIR", call_temp)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(hub_url) = context.hub_url {
        command.env("AGENT_HUB_HUB_URL", hub_url);
    }
    if let Some(token_file) = context.maintenance_token_file {
        command.env("AGENT_HUB_API_KEY_FILE", token_file);
    }
    command.process_group(0);
    let ruleset_fd = sandbox.ruleset.as_raw_fd();
    unsafe {
        command.pre_exec(move || restrict_landlock_child(ruleset_fd));
    }
    let started = Instant::now();
    let mut child = command
        .spawn()
        .with_context(|| format!("spawn Skill executable {}", program.display()))?;
    drop(sandbox);

    let stdin = child.stdin.take().context("open Skill execution stdin")?;
    let stdin_bytes = request.stdin.as_bytes().to_vec();
    let stdin_writer = thread::spawn(move || -> std::io::Result<()> {
        let mut stdin = stdin;
        stdin.write_all(&stdin_bytes)
    });
    let output_limit_exceeded = Arc::new(AtomicBool::new(false));
    let stdout_reader = bounded_reader(
        child.stdout.take().context("open Skill execution stdout")?,
        Arc::clone(&output_limit_exceeded),
    );
    let stderr_reader = bounded_reader(
        child.stderr.take().context("open Skill execution stderr")?,
        Arc::clone(&output_limit_exceeded),
    );
    let timeout = request
        .timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_TIMEOUT);
    let mut timed_out = false;
    let mut limited = false;
    let exit_code = loop {
        if context.stop.load(Ordering::Acquire) || context.disconnected.load(Ordering::Acquire) {
            terminate_child_process_tree(&mut child);
            break None;
        }
        if output_limit_exceeded.load(Ordering::Acquire) {
            limited = true;
            terminate_child_process_tree(&mut child);
            break None;
        }
        if started.elapsed() >= timeout {
            timed_out = true;
            terminate_child_process_tree(&mut child);
            break None;
        }
        if let Some(status) = child.try_wait().context("poll Skill executable")? {
            let exit_code = status.code();
            terminate_child_process_tree(&mut child);
            break exit_code;
        }
        thread::sleep(Duration::from_millis(10));
    };
    let _ = stdin_writer.join();
    let stdout = stdout_reader
        .join()
        .map_err(|_| anyhow::anyhow!("Skill stdout reader panicked"))??;
    let stderr = stderr_reader
        .join()
        .map_err(|_| anyhow::anyhow!("Skill stderr reader panicked"))??;
    anyhow::ensure!(
        !context.stop.load(Ordering::Acquire),
        "Skill execution broker stopped"
    );
    anyhow::ensure!(
        !context.disconnected.load(Ordering::Acquire),
        "Skill execution client disconnected"
    );
    Ok(SkillExecOutput {
        exit_code,
        stdout,
        stderr,
        timed_out,
        output_limit_exceeded: limited,
        elapsed: started.elapsed(),
    })
}

#[cfg(target_os = "linux")]
struct ExecutableLaunch {
    interpreter: Option<PathBuf>,
}

#[cfg(target_os = "linux")]
fn executable_launch(program: &Path) -> anyhow::Result<ExecutableLaunch> {
    let mut file = fs::File::open(program).context("inspect Skill executable format")?;
    let mut header = [0_u8; 512];
    let read = file.read(&mut header)?;
    if !header[..read].starts_with(b"#!") {
        return Ok(ExecutableLaunch { interpreter: None });
    }
    let line_end = header[..read]
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(read);
    let shebang = std::str::from_utf8(&header[2..line_end])
        .context("Skill executable shebang is not UTF-8")?
        .trim();
    let parts = shebang.split_ascii_whitespace().collect::<Vec<_>>();
    anyhow::ensure!(!parts.is_empty(), "Skill executable shebang is empty");
    if parts[0] == "/usr/bin/env" {
        anyhow::ensure!(
            parts.len() == 2,
            "Skill executable env shebang must name one controlled interpreter"
        );
        return Ok(ExecutableLaunch {
            interpreter: Some(resolve_named_interpreter(parts[1])?),
        });
    }
    anyhow::ensure!(
        parts.len() == 1,
        "Skill executable direct shebang cannot include interpreter arguments"
    );
    let interpreter = resolve_direct_interpreter(parts[0])?;
    Ok(ExecutableLaunch {
        interpreter: Some(interpreter),
    })
}

#[cfg(target_os = "linux")]
fn resolve_named_interpreter(name: &str) -> anyhow::Result<PathBuf> {
    let candidates: &[&str] = match name {
        "sh" => &["/bin/sh", "/usr/bin/sh"],
        "bash" => &["/bin/bash", "/usr/bin/bash"],
        "python3" => &["/usr/bin/python3", "/bin/python3"],
        "node" => &["/usr/bin/node", "/bin/node"],
        "bun" => &["/usr/bin/bun", "/bin/bun"],
        _ => anyhow::bail!("Skill executable uses an unsupported interpreter"),
    };
    candidates
        .iter()
        .find_map(|candidate| fs::canonicalize(candidate).ok())
        .context("Skill executable interpreter is not installed")
}

#[cfg(target_os = "linux")]
fn resolve_direct_interpreter(interpreter: &str) -> anyhow::Result<PathBuf> {
    let allowed = [
        "/bin/sh",
        "/usr/bin/sh",
        "/bin/bash",
        "/usr/bin/bash",
        "/usr/bin/python3",
        "/bin/python3",
        "/usr/bin/node",
        "/bin/node",
        "/usr/bin/bun",
        "/bin/bun",
    ];
    anyhow::ensure!(
        allowed.contains(&interpreter),
        "Skill executable uses an unsupported interpreter"
    );
    fs::canonicalize(interpreter).context("resolve Skill executable interpreter")
}

#[cfg(target_os = "linux")]
struct SkillExecFilesystemSandbox {
    ruleset: fs::File,
}

#[cfg(target_os = "linux")]
impl SkillExecFilesystemSandbox {
    fn prepare(
        program: &Path,
        package_root: &Path,
        interpreter: Option<&Path>,
        call_temp: &Path,
        workdir: &Path,
        tools: &[String],
        maintenance_token_file: Option<&Path>,
    ) -> anyhow::Result<Self> {
        let abi_version = super::landlock_abi_version().context("query Linux Landlock ABI")?;
        anyhow::ensure!(
            abi_version >= 2,
            "Skill execution requires Linux Landlock ABI 2 or newer"
        );
        let mut rules = BTreeMap::new();
        PiFilesystemSandbox::add_directory(&mut rules, package_root, LANDLOCK_DIRECTORY_READ)?;
        PiFilesystemSandbox::add_optional_file(&mut rules, program, LANDLOCK_FILE_READ_ONLY)?;
        if let Some(interpreter) = interpreter {
            PiFilesystemSandbox::add_optional_file(
                &mut rules,
                interpreter,
                LANDLOCK_FILE_READ_ONLY,
            )?;
        }
        let workspace_access = skill_exec_workspace_landlock_access(tools);
        if workspace_access != 0 {
            PiFilesystemSandbox::add_directory(&mut rules, workdir, workspace_access)?;
        }
        if let Some(token_file) = maintenance_token_file {
            PiFilesystemSandbox::add_optional_file(&mut rules, token_file, LANDLOCK_FILE_READ)?;
        }
        PiFilesystemSandbox::add_directory(&mut rules, call_temp, LANDLOCK_RUNTIME_WRITE)?;
        for path in [
            "/lib",
            "/lib64",
            "/usr/lib",
            "/usr/lib64",
            "/usr/share/zoneinfo",
            "/usr/share/locale",
            "/etc/ssl/certs",
            "/usr/share/ca-certificates",
        ] {
            PiFilesystemSandbox::add_optional_directory(
                &mut rules,
                Path::new(path),
                LANDLOCK_DIRECTORY_READ,
            )?;
        }
        for path in [
            "/lib64/ld-linux-x86-64.so.2",
            "/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2",
            "/lib/ld-linux-aarch64.so.1",
            "/lib/aarch64-linux-gnu/ld-linux-aarch64.so.1",
            "/lib/ld-musl-x86_64.so.1",
            "/lib/ld-musl-aarch64.so.1",
        ] {
            PiFilesystemSandbox::add_optional_file(
                &mut rules,
                Path::new(path),
                LANDLOCK_FILE_READ_ONLY,
            )?;
        }
        for path in [
            "/etc/ld.so.cache",
            "/etc/localtime",
            "/etc/nsswitch.conf",
            "/etc/hosts",
            "/etc/resolv.conf",
            "/etc/passwd",
            "/etc/group",
            "/etc/ssl/certs/ca-certificates.crt",
            "/dev/urandom",
        ] {
            PiFilesystemSandbox::add_optional_file(
                &mut rules,
                Path::new(path),
                LANDLOCK_FILE_READ,
            )?;
        }
        PiFilesystemSandbox::add_optional_file(
            &mut rules,
            Path::new("/dev/null"),
            LANDLOCK_FILE_READ_WRITE,
        )?;
        for path in ["/bin", "/usr/bin"] {
            PiFilesystemSandbox::add_optional_directory(
                &mut rules,
                Path::new(path),
                LANDLOCK_DIRECTORY_LIST,
            )?;
        }
        let ruleset = create_landlock_ruleset()?;
        for ((path, kind), access) in rules {
            add_landlock_path_rule(&ruleset, &path, kind, access)?;
        }
        Ok(Self { ruleset })
    }
}

#[cfg(target_os = "linux")]
fn skill_exec_workspace_landlock_access(tools: &[String]) -> u64 {
    let mut access = workspace_landlock_access(tools) & !LANDLOCK_ACCESS_FS_EXECUTE;
    let readable = tools.iter().any(|tool| {
        matches!(
            tool.as_str(),
            "read" | "grep" | "find" | "ls" | "edit" | "bash"
        )
    });
    if !readable {
        access &= !(LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_READ_DIR);
    }
    access
}

#[cfg(target_os = "linux")]
fn bounded_reader<R: Read + Send + 'static>(
    mut reader: R,
    exceeded: Arc<AtomicBool>,
) -> thread::JoinHandle<std::io::Result<Vec<u8>>> {
    thread::spawn(move || {
        let mut output = Vec::new();
        let mut chunk = [0_u8; 8192];
        loop {
            let read = reader.read(&mut chunk)?;
            if read == 0 {
                return Ok(output);
            }
            let remaining = MAX_OUTPUT_BYTES.saturating_sub(output.len());
            output.extend_from_slice(&chunk[..read.min(remaining)]);
            if read > remaining {
                exceeded.store(true, Ordering::Release);
            }
        }
    })
}

#[cfg(target_os = "linux")]
fn monitor_disconnect(
    mut stream: UnixStream,
    disconnected: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let _ = stream.set_read_timeout(Some(Duration::from_millis(25)));
        let mut byte = [0_u8; 1];
        while !done.load(Ordering::Acquire) {
            match stream.read(&mut byte) {
                Ok(0) | Ok(_) => {
                    disconnected.store(true, Ordering::Release);
                    return;
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => {
                    disconnected.store(true, Ordering::Release);
                    return;
                }
            }
        }
    })
}

#[cfg(target_os = "linux")]
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(target_os = "linux")]
fn remove_socket_if_present(path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                !metadata.is_dir(),
                "Skill execution socket path is a directory"
            );
            fs::remove_file(path)
                .with_context(|| format!("remove Skill execution socket {}", path.display()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("inspect Skill execution socket"),
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::{
        io::BufRead,
        os::unix::fs::{symlink, PermissionsExt},
        sync::atomic::AtomicBool,
    };

    use serde_json::json;

    use super::*;

    struct Fixture {
        _temp: tempfile::TempDir,
        run_env: RunEnv,
        packages_root: PathBuf,
        catalog_path: PathBuf,
        program: PathBuf,
        sibling_secret: PathBuf,
    }

    fn fixture(script: &str) -> Fixture {
        let temp = tempfile::tempdir().unwrap();
        let session = temp.path().join("sessions/first");
        let run_env = RunEnv {
            workdir: session.join("workspace"),
            engine_state_root: session.join("engine-state"),
            hub_url: "http://127.0.0.1:8080".into(),
            maintenance_token_file: None,
        };
        fs::create_dir_all(&run_env.workdir).unwrap();
        fs::create_dir_all(super::super::pi_temp_directory(&run_env)).unwrap();
        let root = run_env.engine_state_root.join(SKILL_EXEC_DIRECTORY);
        let packages_root = root.join(SKILL_EXEC_PACKAGES_DIRECTORY);
        let package_root = packages_root.join("package-one");
        let program = package_root.join("bin/client");
        fs::create_dir_all(program.parent().unwrap()).unwrap();
        fs::create_dir_all(root.join(SKILL_EXEC_TEMP_DIRECTORY)).unwrap();
        fs::write(&program, script).unwrap();
        fs::set_permissions(&program, fs::Permissions::from_mode(0o500)).unwrap();
        let sibling_secret = temp.path().join("sessions/second/workspace/secret.txt");
        fs::create_dir_all(sibling_secret.parent().unwrap()).unwrap();
        fs::write(&sibling_secret, "sibling-secret\n").unwrap();
        let catalog_path = root.join(SKILL_EXEC_CATALOG_FILE);
        fs::write(
            &catalog_path,
            serde_json::to_vec(&json!({
                "version": 1,
                "skills": [{
                    "name": "deploy",
                    "package_id": uuid::Uuid::new_v4(),
                    "package_root": package_root,
                    "executables": ["bin/client"]
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::set_permissions(&catalog_path, fs::Permissions::from_mode(0o600)).unwrap();
        Fixture {
            _temp: temp,
            run_env,
            packages_root,
            catalog_path,
            program,
            sibling_secret,
        }
    }

    fn request(args: Vec<String>, timeout_ms: Option<u64>) -> SkillExecRequest {
        SkillExecRequest {
            token: "token".into(),
            skill: "deploy".into(),
            program: "bin/client".into(),
            args,
            stdin: "input-value\n".into(),
            timeout_ms,
        }
    }

    #[test]
    fn executes_only_catalog_program_with_private_temp_and_session_isolation() {
        let fixture = fixture(
            r#"#!/bin/sh
set -eu
IFS= read -r input
IFS= read -r own < "$1"
if /bin/sh -c 'IFS= read -r forbidden < "$1"' sh "$2"; then exit 91; fi
if /bin/sh -c ': > "$1"' sh "$3"; then exit 92; fi
if /bin/true; then exit 93; fi
printf 'temp-data\n' > "$TMPDIR/generated"
printf 'input=%s own=%s temp=%s\n' "$input" "$own" "$(IFS= read -r value < "$TMPDIR/generated"; printf %s "$value")"
"#,
        );
        let own = fixture.run_env.workdir.join("own.txt");
        let denied_write = fixture.run_env.workdir.join("denied.txt");
        fs::write(&own, "own-value\n").unwrap();
        let request = request(
            vec![
                own.to_string_lossy().into_owned(),
                fixture.sibling_secret.to_string_lossy().into_owned(),
                denied_write.to_string_lossy().into_owned(),
            ],
            None,
        );
        let stop = AtomicBool::new(false);
        let disconnected = AtomicBool::new(false);
        let temp_root = fixture
            .run_env
            .engine_state_root
            .join(SKILL_EXEC_DIRECTORY)
            .join(SKILL_EXEC_TEMP_DIRECTORY);
        let tools = ["read".into(), "skill_exec".into()];
        let context = SkillExecExecutionContext {
            packages_root: &fixture.packages_root,
            temp_root: &temp_root,
            workdir: &fixture.run_env.workdir,
            tools: &tools,
            stop: &stop,
            disconnected: &disconnected,
            hub_url: None,
            maintenance_token_file: None,
        };
        let output = execute_program(&fixture.program, &request, &context).unwrap();
        assert_eq!(
            output.exit_code,
            Some(0),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            "input=input-value own=own-value temp=temp-data\n"
        );
        assert!(!denied_write.exists());
    }

    #[test]
    fn does_not_grant_workspace_read_access_without_a_read_capable_tool() {
        let fixture = fixture(
            r#"#!/bin/sh
if /bin/sh -c 'IFS= read -r leaked < "$1"' sh "$1"; then exit 91; fi
printf 'workspace-read-denied\n'
"#,
        );
        let secret = fixture.run_env.workdir.join("secret.txt");
        fs::write(&secret, "workspace-secret\n").unwrap();
        let request = request(vec![secret.to_string_lossy().into_owned()], None);
        let stop = AtomicBool::new(false);
        let disconnected = AtomicBool::new(false);
        let temp_root = fixture
            .run_env
            .engine_state_root
            .join(SKILL_EXEC_DIRECTORY)
            .join(SKILL_EXEC_TEMP_DIRECTORY);
        let tools = ["skill_exec".into()];
        let context = SkillExecExecutionContext {
            packages_root: &fixture.packages_root,
            temp_root: &temp_root,
            workdir: &fixture.run_env.workdir,
            tools: &tools,
            stop: &stop,
            disconnected: &disconnected,
            hub_url: None,
            maintenance_token_file: None,
        };

        let output = execute_program(&fixture.program, &request, &context).unwrap();

        assert_eq!(output.exit_code, Some(0));
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            "workspace-read-denied\n"
        );
    }

    #[test]
    fn rejects_direct_shebang_interpreter_arguments() {
        let fixture = fixture("#!/bin/sh outside.sh\nprintf should-not-run\n");
        let request = request(Vec::new(), None);
        let stop = AtomicBool::new(false);
        let disconnected = AtomicBool::new(false);
        let temp_root = fixture
            .run_env
            .engine_state_root
            .join(SKILL_EXEC_DIRECTORY)
            .join(SKILL_EXEC_TEMP_DIRECTORY);
        let tools = ["read".into(), "skill_exec".into()];
        let context = SkillExecExecutionContext {
            packages_root: &fixture.packages_root,
            temp_root: &temp_root,
            workdir: &fixture.run_env.workdir,
            tools: &tools,
            stop: &stop,
            disconnected: &disconnected,
            hub_url: None,
            maintenance_token_file: None,
        };

        assert!(execute_program(&fixture.program, &request, &context)
            .err()
            .expect("direct shebang arguments must be rejected")
            .to_string()
            .contains("cannot include interpreter arguments"));
    }

    #[test]
    fn reaps_background_processes_after_the_catalog_program_exits() {
        let fixture = fixture(
            r#"#!/bin/sh
( while :; do :; done ) </dev/null >/dev/null 2>&1 &
printf '%s\n' "$!" > "$1"
"#,
        );
        let pid_file = fixture.run_env.workdir.join("background.pid");
        let request = request(vec![pid_file.to_string_lossy().into_owned()], None);
        let stop = AtomicBool::new(false);
        let disconnected = AtomicBool::new(false);
        let temp_root = fixture
            .run_env
            .engine_state_root
            .join(SKILL_EXEC_DIRECTORY)
            .join(SKILL_EXEC_TEMP_DIRECTORY);
        let tools = ["write".into(), "skill_exec".into()];
        let context = SkillExecExecutionContext {
            packages_root: &fixture.packages_root,
            temp_root: &temp_root,
            workdir: &fixture.run_env.workdir,
            tools: &tools,
            stop: &stop,
            disconnected: &disconnected,
            hub_url: None,
            maintenance_token_file: None,
        };

        let output = execute_program(&fixture.program, &request, &context).unwrap();
        assert_eq!(output.exit_code, Some(0));
        let pid = fs::read_to_string(pid_file)
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();
        let mut exited = false;
        for _ in 0..50 {
            let result = unsafe { libc::kill(pid, 0) };
            if result < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                exited = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        if !exited {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
        assert!(exited, "Skill background process {pid} survived the call");
    }

    #[test]
    fn rejects_uncatalogued_and_cross_session_programs() {
        let fixture = fixture("#!/bin/sh\nprintf should-not-run\n");
        let catalog = load_catalog(&fixture.catalog_path, &fixture.packages_root).unwrap();
        assert!(
            resolve_program(&catalog, &fixture.packages_root, "deploy", "bin/other")
                .unwrap_err()
                .to_string()
                .contains("not executable")
        );
        assert!(validate_executable_path("../bin/client").is_err());

        let outside = fixture
            .run_env
            .engine_state_root
            .join("other-package/bin/client");
        fs::create_dir_all(outside.parent().unwrap()).unwrap();
        fs::write(&outside, "#!/bin/sh\n").unwrap();
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o500)).unwrap();
        fs::write(
            &fixture.catalog_path,
            serde_json::to_vec(&json!({
                "version": 1,
                "skills": [{
                    "name": "deploy",
                    "package_id": uuid::Uuid::new_v4(),
                    "package_root": outside.parent().unwrap().parent().unwrap(),
                    "executables": ["bin/client"]
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(load_catalog(&fixture.catalog_path, &fixture.packages_root)
            .unwrap_err()
            .to_string()
            .contains("outside the current Session"));
    }

    #[test]
    fn terminates_program_at_requested_timeout() {
        let fixture = fixture("#!/bin/sh\nwhile :; do :; done\n");
        let request = request(Vec::new(), Some(50));
        let stop = AtomicBool::new(false);
        let disconnected = AtomicBool::new(false);
        let temp_root = fixture
            .run_env
            .engine_state_root
            .join(SKILL_EXEC_DIRECTORY)
            .join(SKILL_EXEC_TEMP_DIRECTORY);
        let tools = ["read".into(), "skill_exec".into()];
        let context = SkillExecExecutionContext {
            packages_root: &fixture.packages_root,
            temp_root: &temp_root,
            workdir: &fixture.run_env.workdir,
            tools: &tools,
            stop: &stop,
            disconnected: &disconnected,
            hub_url: None,
            maintenance_token_file: None,
        };
        let output = execute_program(&fixture.program, &request, &context).unwrap();
        assert!(output.timed_out);
        assert!(output.elapsed < Duration::from_secs(2));
    }

    #[test]
    fn broker_rejects_an_invalid_capability_token() {
        let fixture = fixture("#!/bin/sh\nprintf should-not-run\n");
        let broker = SkillExecBroker::start(
            &fixture.run_env,
            &["read".into(), "skill_exec".into()],
            "http://127.0.0.1:8080",
            None,
        )
        .unwrap();
        let mut stream = UnixStream::connect(broker.socket_path()).unwrap();
        let encoded = serde_json::to_vec(&request(Vec::new(), None)).unwrap();
        stream.write_all(&encoded).unwrap();
        stream.write_all(b"\n").unwrap();
        let mut response = String::new();
        std::io::BufReader::new(stream)
            .read_line(&mut response)
            .unwrap();
        let response: SkillExecResponse = serde_json::from_str(&response).unwrap();
        assert!(!response.ok);
        assert_eq!(
            response.error.as_deref(),
            Some("invalid Skill execution token")
        );
    }

    #[test]
    fn broker_socket_supports_long_session_paths() {
        let mut fixture = fixture("#!/bin/sh\nprintf should-not-run\n");
        let session_root = fixture
            .run_env
            .engine_state_root
            .parent()
            .unwrap()
            .to_path_buf();
        let long_alias = fixture._temp.path().join("long-session-root-".repeat(8));
        symlink(session_root, &long_alias).unwrap();
        fixture.run_env.workdir = long_alias.join("workspace");
        fixture.run_env.engine_state_root = long_alias.join("engine-state");
        let session_local_socket =
            super::super::pi_temp_directory(&fixture.run_env).join(SKILL_EXEC_SOCKET_FILE);
        assert!(session_local_socket.to_string_lossy().len() >= 108);

        let broker = SkillExecBroker::start(
            &fixture.run_env,
            &["read".into(), "skill_exec".into()],
            "http://127.0.0.1:8080",
            None,
        )
        .unwrap();

        assert!(broker.socket_path().to_string_lossy().len() < 108);
    }

    fn write_catalog_name(fixture: &Fixture, name: &str) {
        let package_root = fixture.program.parent().unwrap().parent().unwrap();
        fs::write(
            &fixture.catalog_path,
            serde_json::to_vec(&json!({
                "version": 1,
                "skills": [{
                    "name": name,
                    "package_id": uuid::Uuid::new_v4(),
                    "package_root": package_root,
                    "executables": ["bin/client"]
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::set_permissions(&fixture.catalog_path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn send_broker_request(broker: &SkillExecBroker, skill: &str) -> SkillExecResponse {
        let mut stream = UnixStream::connect(broker.socket_path()).unwrap();
        let request = SkillExecRequest {
            token: broker.token().to_owned(),
            skill: skill.into(),
            program: "bin/client".into(),
            args: Vec::new(),
            stdin: String::new(),
            timeout_ms: None,
        };
        stream
            .write_all(&serde_json::to_vec(&request).unwrap())
            .unwrap();
        stream.write_all(b"\n").unwrap();
        let mut response = String::new();
        std::io::BufReader::new(stream)
            .read_line(&mut response)
            .unwrap();
        serde_json::from_str(&response).unwrap()
    }

    #[test]
    fn maintenance_skill_receives_hub_url_and_token_file() {
        let mut fixture = fixture(
            "#!/bin/sh\nprintf '%s|%s|' \"$AGENT_HUB_HUB_URL\" \"$AGENT_HUB_API_KEY_FILE\"; IFS= read -r key < \"$AGENT_HUB_API_KEY_FILE\"; printf '%s' \"$key\"\n",
        );
        let token_file = fixture._temp.path().join("maintenance-token");
        fs::write(&token_file, "maintenance-key\n").unwrap();
        fs::set_permissions(&token_file, fs::Permissions::from_mode(0o400)).unwrap();
        fixture.run_env.maintenance_token_file = Some(token_file.clone());
        write_catalog_name(&fixture, AGENT_HUB_MAINTENANCE_SKILL_NAME);
        let broker = SkillExecBroker::start(
            &fixture.run_env,
            &["read".into(), "skill_exec".into()],
            "http://hub.internal",
            Some(&token_file),
        )
        .unwrap();
        let response = send_broker_request(&broker, AGENT_HUB_MAINTENANCE_SKILL_NAME);
        assert!(response.ok, "{}", response.error.unwrap_or_default());
        assert_eq!(
            response.stdout,
            format!(
                "http://hub.internal|{}|maintenance-key",
                token_file.display()
            )
        );
    }

    #[test]
    fn non_maintenance_skill_does_not_receive_hub_credentials() {
        let fixture = fixture(
            "#!/bin/sh\nprintf '%s|%s' \"$AGENT_HUB_HUB_URL\" \"$AGENT_HUB_API_KEY_FILE\"\n",
        );
        let token_file = fixture._temp.path().join("maintenance-token");
        fs::write(&token_file, "maintenance-key\n").unwrap();
        fs::set_permissions(&token_file, fs::Permissions::from_mode(0o400)).unwrap();
        write_catalog_name(&fixture, "deploy");
        let broker = SkillExecBroker::start(
            &fixture.run_env,
            &["read".into(), "skill_exec".into()],
            "http://hub.internal",
            Some(&token_file),
        )
        .unwrap();
        let response = send_broker_request(&broker, "deploy");
        assert!(response.ok, "{}", response.error.unwrap_or_default());
        assert_eq!(response.stdout, "|");
    }
}
