use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    env, fmt, fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{mpsc, Arc},
    time::{Duration, Instant},
};
#[cfg(target_os = "linux")]
use std::{
    ffi::CString,
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::{ffi::OsStrExt, process::CommandExt},
    },
};

use agent_hub_shared::{AgentDto, AppendRunEventRequest, ClaimRunResponse, IntegrationContextDto};
use anyhow::Context;
use serde_json::{json, Value};
use tokio::sync::mpsc as tokio_mpsc;

use super::{
    model_binding, pi_model_provider_name, pi_thinking_level, send_engine_event_with_backpressure,
    stable_tool_request_uuid, terminate_child_process_tree, EngineCancellation, EngineRunResult,
    PendingInterruptResponse, PendingSteerResponse, RunEnv, SessionInterruptOutcome,
    SessionSteerOutcome, SessionSupervisorCommand, ENGINE_EVENT_QUEUE_CAPACITY,
};

mod skill_exec;

use skill_exec::{SkillExecBroker, SKILL_EXEC_EXTENSION_SOURCE};

const PI_SESSION_DIRECTORY: &str = "sessions";
const PI_AGENT_DIRECTORY: &str = ".pi/agent";
const PI_HOME_DIRECTORY: &str = ".pi/home";
const PI_TEMP_DIRECTORY: &str = ".pi/tmp";
const PI_PROCESS_PATH: &str = "/usr/bin:/bin";
const PI_INTEGRATION_EXTENSION_FILE: &str = "agent-hub-integration-tools.mjs";
const PI_INTEGRATION_TOOLS_FILE: &str = "agent-hub-integration-tools.json";
const PI_SKILL_EXEC_EXTENSION_FILE: &str = "agent-hub-skill-exec.mjs";
const PI_INTEGRATION_CONTEXT_LABEL: &str = "Agent Hub Integration context (JSON):";
const PI_CLIENT_TOOL_PREFIX: &str = "agent_hub_client_tool_";
const PI_BUILTIN_TOOL_NAMES: &[&str] = &[
    "read",
    "grep",
    "find",
    "ls",
    "edit",
    "write",
    "bash",
    "skill_exec",
];

#[derive(Debug)]
pub(super) struct PiRpcTimeout {
    timeout: Duration,
}

impl PiRpcTimeout {
    pub(super) fn timeout_seconds(&self) -> u64 {
        self.timeout.as_secs()
    }
}

impl fmt::Display for PiRpcTimeout {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Pi RPC process timed out after {:?}",
            self.timeout
        )
    }
}

impl std::error::Error for PiRpcTimeout {}
const PI_INTEGRATION_EXTENSION_SOURCE: &str = r#"import { readFileSync } from "node:fs";

const definitions = JSON.parse(
  readFileSync(new URL("./agent-hub-integration-tools.json", import.meta.url), "utf8"),
);

export default function registerAgentHubIntegrationTools(pi) {
  for (const definition of definitions) {
    pi.registerTool({
      name: definition.name,
      label: definition.label,
      description: definition.description,
      parameters: definition.parameters,
      async execute() {
        return {
          content: [{
            type: "text",
            text: "Integration tool request delegated to Agent Hub.",
          }],
          details: { pending: true },
          terminate: true,
        };
      },
    });
  }
}
"#;

fn pi_agent_directory(run_env: &RunEnv) -> PathBuf {
    run_env.engine_state_root.join(PI_AGENT_DIRECTORY)
}

fn pi_home_directory(run_env: &RunEnv) -> PathBuf {
    run_env.engine_state_root.join(PI_HOME_DIRECTORY)
}

fn pi_temp_directory(run_env: &RunEnv) -> PathBuf {
    run_env.engine_state_root.join(PI_TEMP_DIRECTORY)
}

fn pi_integration_extension_path(run_env: &RunEnv) -> PathBuf {
    pi_agent_directory(run_env).join(PI_INTEGRATION_EXTENSION_FILE)
}

fn pi_integration_tools_path(run_env: &RunEnv) -> PathBuf {
    pi_agent_directory(run_env).join(PI_INTEGRATION_TOOLS_FILE)
}

fn pi_skill_exec_extension_path(run_env: &RunEnv) -> PathBuf {
    pi_agent_directory(run_env).join(PI_SKILL_EXEC_EXTENSION_FILE)
}

fn materialize_skill_exec_extension(run_env: &RunEnv, enabled: bool) -> anyhow::Result<()> {
    let extension_path = pi_skill_exec_extension_path(run_env);
    if !enabled {
        return remove_file_if_present(&extension_path);
    }
    prepare_private_directory(&pi_agent_directory(run_env), "Pi Agent directory")?;
    replace_private_file(&extension_path, SKILL_EXEC_EXTENSION_SOURCE.as_bytes())
        .context("write Skill execution Extension")
}

fn normalized_integration_tools(
    context: Option<&IntegrationContextDto>,
) -> anyhow::Result<Vec<Value>> {
    let Some(context) = context else {
        return Ok(Vec::new());
    };
    let tools = context
        .tools
        .as_array()
        .context("Integration tools must be an array")?;
    let mut names = HashSet::new();
    let mut parsed = Vec::with_capacity(tools.len());
    for tool in tools {
        let external_name = tool
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty())
            .context("Integration tool name is required")?;
        anyhow::ensure!(
            !external_name.contains(','),
            "Integration tool name cannot contain a comma"
        );
        let client_managed = tool.get("input_schema").is_some();
        if !client_managed {
            anyhow::ensure!(
                !PI_BUILTIN_TOOL_NAMES.contains(&external_name),
                "Integration tool name conflicts with a Pi built-in tool"
            );
        }
        anyhow::ensure!(
            names.insert(external_name.to_owned()),
            "Integration tool names must be unique"
        );
        let description = match tool.get("description") {
            Some(Value::String(description)) => description.clone(),
            Some(Value::Null) | None => String::new(),
            Some(_) => anyhow::bail!("Integration tool description must be a string"),
        };
        let parameters = match if client_managed {
            tool.get("input_schema")
        } else {
            tool.get("parameters")
        } {
            Some(parameters @ Value::Object(_)) => parameters.clone(),
            Some(Value::Null) | None => json!({ "type": "object" }),
            Some(_) => anyhow::bail!("Integration tool parameters must be a JSON object"),
        };
        parsed.push((
            external_name.to_owned(),
            description,
            parameters,
            client_managed,
        ));
    }
    let mut allocated_names = names;
    let mut next_client_name = 1_u32;
    let mut normalized = Vec::with_capacity(parsed.len());
    for (external_name, description, parameters, client_managed) in parsed {
        let internal_name = if client_managed {
            loop {
                let candidate = format!("{PI_CLIENT_TOOL_PREFIX}{next_client_name}");
                next_client_name = next_client_name
                    .checked_add(1)
                    .context("Client Tool internal name counter overflowed")?;
                if !allocated_names.contains(&candidate)
                    && !PI_BUILTIN_TOOL_NAMES.contains(&candidate.as_str())
                {
                    allocated_names.insert(candidate.clone());
                    break candidate;
                }
            }
        } else {
            external_name.clone()
        };
        normalized.push(json!({
            "name": internal_name,
            "label": external_name,
            "external_name": external_name,
            "client_managed": client_managed,
            "description": description,
            "parameters": parameters
        }));
    }
    Ok(normalized)
}

fn integration_tool_name_map(
    context: Option<&IntegrationContextDto>,
) -> anyhow::Result<BTreeMap<String, String>> {
    normalized_integration_tools(context).map(|tools| {
        tools
            .into_iter()
            .filter_map(|tool| {
                Some((
                    tool.get("name")?.as_str()?.to_owned(),
                    tool.get("external_name")?.as_str()?.to_owned(),
                ))
            })
            .collect()
    })
}

fn remove_file_if_present(path: &Path) -> anyhow::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

fn replace_private_file(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let parent = path
            .parent()
            .context("private file has no parent directory")?;
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .context("private file name is not valid UTF-8")?;
        let temporary = parent.join(format!(".{filename}.{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| -> anyhow::Result<()> {
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&temporary)
                .with_context(|| {
                    format!("create temporary private file {}", temporary.display())
                })?;
            file.write_all(contents)
                .with_context(|| format!("write temporary private file {}", temporary.display()))?;
            drop(file);
            fs::rename(&temporary, path)
                .with_context(|| format!("replace private file {}", path.display()))?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    #[cfg(not(unix))]
    {
        let _ = (path, contents);
        anyhow::bail!("private config files require a Unix runtime");
    }
}

pub(super) fn materialize_integration_tools(
    run_env: &RunEnv,
    context: Option<&IntegrationContextDto>,
) -> anyhow::Result<()> {
    let tools = normalized_integration_tools(context)?;
    let extension_path = pi_integration_extension_path(run_env);
    let tools_path = pi_integration_tools_path(run_env);
    if tools.is_empty() {
        remove_file_if_present(&extension_path)?;
        remove_file_if_present(&tools_path)?;
        return Ok(());
    }

    prepare_private_directory(&pi_agent_directory(run_env), "Pi Agent directory")?;
    replace_private_file(&tools_path, &serde_json::to_vec_pretty(&tools)?)
        .context("write Integration tools catalog")?;
    replace_private_file(&extension_path, PI_INTEGRATION_EXTENSION_SOURCE.as_bytes())
        .context("write Integration tools Extension")?;
    Ok(())
}

fn prepare_private_directory(path: &Path, purpose: &str) -> anyhow::Result<()> {
    fs::create_dir_all(path).with_context(|| format!("create {purpose}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("protect {purpose}"))?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
const LANDLOCK_CREATE_RULESET_VERSION: u32 = 1;
#[cfg(target_os = "linux")]
const LANDLOCK_RULE_PATH_BENEATH: u32 = 1;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_EXECUTE: u64 = 1 << 0;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_WRITE_FILE: u64 = 1 << 1;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_READ_FILE: u64 = 1 << 2;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_READ_DIR: u64 = 1 << 3;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_REMOVE_DIR: u64 = 1 << 4;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_REMOVE_FILE: u64 = 1 << 5;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_MAKE_DIR: u64 = 1 << 7;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_MAKE_REG: u64 = 1 << 8;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_MAKE_SOCK: u64 = 1 << 9;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_MAKE_FIFO: u64 = 1 << 10;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_MAKE_SYM: u64 = 1 << 12;
#[cfg(target_os = "linux")]
const LANDLOCK_ACCESS_FS_REFER: u64 = 1 << 13;
#[cfg(target_os = "linux")]
const LANDLOCK_ALL_FILESYSTEM_ACCESS: u64 = LANDLOCK_ACCESS_FS_EXECUTE
    | LANDLOCK_ACCESS_FS_WRITE_FILE
    | LANDLOCK_ACCESS_FS_READ_FILE
    | LANDLOCK_ACCESS_FS_READ_DIR
    | LANDLOCK_ACCESS_FS_REMOVE_DIR
    | LANDLOCK_ACCESS_FS_REMOVE_FILE
    | (1 << 6)
    | LANDLOCK_ACCESS_FS_MAKE_DIR
    | LANDLOCK_ACCESS_FS_MAKE_REG
    | LANDLOCK_ACCESS_FS_MAKE_SOCK
    | LANDLOCK_ACCESS_FS_MAKE_FIFO
    | (1 << 11)
    | LANDLOCK_ACCESS_FS_MAKE_SYM
    | LANDLOCK_ACCESS_FS_REFER;
#[cfg(target_os = "linux")]
const LANDLOCK_DIRECTORY_READ_ONLY: u64 =
    LANDLOCK_ACCESS_FS_EXECUTE | LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_READ_DIR;
#[cfg(target_os = "linux")]
const LANDLOCK_DIRECTORY_LIST: u64 = LANDLOCK_ACCESS_FS_READ_DIR;
#[cfg(target_os = "linux")]
const LANDLOCK_FILE_READ_ONLY: u64 = LANDLOCK_ACCESS_FS_EXECUTE | LANDLOCK_ACCESS_FS_READ_FILE;
#[cfg(target_os = "linux")]
const LANDLOCK_FILE_READ: u64 = LANDLOCK_ACCESS_FS_READ_FILE;
#[cfg(target_os = "linux")]
const LANDLOCK_FILE_READ_WRITE: u64 = LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_WRITE_FILE;
#[cfg(target_os = "linux")]
const LANDLOCK_RUNTIME_WRITE: u64 = LANDLOCK_ACCESS_FS_READ_FILE
    | LANDLOCK_ACCESS_FS_READ_DIR
    | LANDLOCK_ACCESS_FS_WRITE_FILE
    | LANDLOCK_ACCESS_FS_REMOVE_DIR
    | LANDLOCK_ACCESS_FS_REMOVE_FILE
    | LANDLOCK_ACCESS_FS_MAKE_DIR
    | LANDLOCK_ACCESS_FS_MAKE_REG
    | LANDLOCK_ACCESS_FS_MAKE_SOCK
    | LANDLOCK_ACCESS_FS_MAKE_FIFO
    | LANDLOCK_ACCESS_FS_MAKE_SYM
    | LANDLOCK_ACCESS_FS_REFER;

#[cfg(target_os = "linux")]
#[repr(C)]
struct LandlockRulesetAttr {
    handled_access_fs: u64,
}

#[cfg(target_os = "linux")]
#[repr(C, packed)]
struct LandlockPathBeneathAttr {
    allowed_access: u64,
    parent_fd: i32,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum LandlockPathKind {
    Directory,
    File,
}

#[cfg(target_os = "linux")]
struct PiFilesystemSandbox {
    ruleset: fs::File,
}

#[cfg(target_os = "linux")]
impl PiFilesystemSandbox {
    fn prepare(
        pi_bin: &str,
        run_env: &RunEnv,
        agent_dir: &Path,
        session_dir: &Path,
        pi_home: &Path,
        pi_temp: &Path,
        tools: &[String],
    ) -> anyhow::Result<Self> {
        let abi_version = landlock_abi_version().context("query Linux Landlock ABI")?;
        anyhow::ensure!(
            abi_version >= 2,
            "Pi Runtime requires Linux Landlock ABI 2 or newer, found ABI {abi_version}"
        );

        let mut rules = BTreeMap::new();
        let workspace_access = workspace_landlock_access(tools);
        Self::add_directory(&mut rules, &run_env.workdir, workspace_access)?;
        let secret_readable = tools
            .iter()
            .any(|tool| matches!(tool.as_str(), "read" | "grep" | "find" | "ls"))
            && !tools
                .iter()
                .any(|tool| matches!(tool.as_str(), "bash" | "edit" | "write"));
        if secret_readable {
            Self::add_optional_directory(
                &mut rules,
                &run_env.engine_state_root.join("secrets"),
                LANDLOCK_DIRECTORY_LIST | LANDLOCK_ACCESS_FS_READ_FILE,
            )?;
        }
        for path in [agent_dir, session_dir, pi_home, pi_temp] {
            Self::add_directory(&mut rules, path, LANDLOCK_RUNTIME_WRITE)?;
        }

        let pi_binary =
            fs::canonicalize(pi_bin).with_context(|| format!("resolve Pi executable {pi_bin}"))?;
        let pi_runtime_root = pi_binary
            .parent()
            .context("Pi executable has no parent directory")?;
        Self::add_directory(&mut rules, pi_runtime_root, LANDLOCK_DIRECTORY_READ_ONLY)?;

        for path in [
            "/lib",
            "/lib64",
            "/usr/lib",
            "/usr/lib64",
            "/usr/share/zoneinfo",
        ] {
            Self::add_optional_directory(
                &mut rules,
                Path::new(path),
                LANDLOCK_DIRECTORY_READ_ONLY,
            )?;
        }
        for path in ["/etc/ssl/certs", "/usr/share/ca-certificates"] {
            Self::add_optional_directory(
                &mut rules,
                Path::new(path),
                LANDLOCK_DIRECTORY_LIST | LANDLOCK_ACCESS_FS_READ_FILE,
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
            Self::add_optional_file(&mut rules, Path::new(path), LANDLOCK_FILE_READ)?;
        }
        Self::add_optional_file(&mut rules, Path::new("/dev/null"), LANDLOCK_FILE_READ_WRITE)?;

        let bash_enabled = tools.iter().any(|tool| tool == "bash");
        if bash_enabled {
            for path in ["/bin", "/usr/bin"] {
                Self::add_optional_directory(
                    &mut rules,
                    Path::new(path),
                    LANDLOCK_DIRECTORY_READ_ONLY,
                )?;
            }
        } else {
            for path in ["/bin", "/usr/bin"] {
                Self::add_optional_directory(&mut rules, Path::new(path), LANDLOCK_DIRECTORY_LIST)?;
            }
            for path in [
                "/bin/sh",
                "/bin/bash",
                "/usr/bin/rg",
                "/usr/bin/fd",
                "/usr/bin/fdfind",
            ] {
                Self::add_optional_file(&mut rules, Path::new(path), LANDLOCK_FILE_READ_ONLY)?;
            }
        }

        #[cfg(test)]
        {
            // Existing RPC fixtures are shell wrappers created under a temporary
            // directory. This compatibility allowance is absent from release builds;
            // isolation regression tests use /bin/sh and therefore do not receive it.
            if pi_binary.starts_with(env::temp_dir()) {
                Self::add_directory(&mut rules, pi_runtime_root, LANDLOCK_RUNTIME_WRITE)?;
                let fixture =
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/fake-pi-rpc.sh");
                Self::add_optional_file(&mut rules, &fixture, LANDLOCK_FILE_READ_ONLY)?;
                for dependency in [
                    "/usr/bin/basename",
                    "/usr/bin/date",
                    "/usr/bin/jq",
                    "/usr/bin/mkdir",
                ] {
                    Self::add_optional_file(
                        &mut rules,
                        Path::new(dependency),
                        LANDLOCK_FILE_READ_ONLY,
                    )?;
                }
            }
        }

        let ruleset = create_landlock_ruleset()?;
        for ((path, kind), access) in rules {
            add_landlock_path_rule(&ruleset, &path, kind, access)?;
        }
        Ok(Self { ruleset })
    }

    fn ruleset_fd(&self) -> RawFd {
        self.ruleset.as_raw_fd()
    }

    fn add_directory(
        rules: &mut BTreeMap<(PathBuf, LandlockPathKind), u64>,
        path: &Path,
        access: u64,
    ) -> anyhow::Result<()> {
        let canonical = fs::canonicalize(path)
            .with_context(|| format!("resolve Landlock directory {}", path.display()))?;
        anyhow::ensure!(
            fs::metadata(&canonical)
                .with_context(|| format!("inspect Landlock directory {}", canonical.display()))?
                .is_dir(),
            "Landlock path is not a directory: {}",
            canonical.display()
        );
        *rules
            .entry((canonical, LandlockPathKind::Directory))
            .or_default() |= access;
        Ok(())
    }

    fn add_optional_directory(
        rules: &mut BTreeMap<(PathBuf, LandlockPathKind), u64>,
        path: &Path,
        access: u64,
    ) -> anyhow::Result<()> {
        match fs::symlink_metadata(path) {
            Ok(_) => Self::add_directory(rules, path, access),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                Err(error).with_context(|| format!("inspect Landlock directory {}", path.display()))
            }
        }
    }

    fn add_optional_file(
        rules: &mut BTreeMap<(PathBuf, LandlockPathKind), u64>,
        path: &Path,
        access: u64,
    ) -> anyhow::Result<()> {
        match fs::symlink_metadata(path) {
            Ok(_) => {
                let canonical = fs::canonicalize(path)
                    .with_context(|| format!("resolve Landlock file {}", path.display()))?;
                anyhow::ensure!(
                    !fs::metadata(&canonical)
                        .with_context(|| format!("inspect Landlock file {}", canonical.display()))?
                        .is_dir(),
                    "Landlock file path is a directory: {}",
                    canonical.display()
                );
                *rules
                    .entry((canonical, LandlockPathKind::File))
                    .or_default() |= access;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                Err(error).with_context(|| format!("inspect Landlock file {}", path.display()))
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn workspace_landlock_access(tools: &[String]) -> u64 {
    let mut access = LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_READ_DIR;
    let writable = tools
        .iter()
        .any(|tool| matches!(tool.as_str(), "edit" | "write" | "bash"));
    if writable {
        access |= LANDLOCK_ACCESS_FS_WRITE_FILE
            | LANDLOCK_ACCESS_FS_REMOVE_DIR
            | LANDLOCK_ACCESS_FS_REMOVE_FILE
            | LANDLOCK_ACCESS_FS_MAKE_DIR
            | LANDLOCK_ACCESS_FS_MAKE_REG
            | LANDLOCK_ACCESS_FS_MAKE_SOCK
            | LANDLOCK_ACCESS_FS_MAKE_FIFO
            | LANDLOCK_ACCESS_FS_MAKE_SYM
            | LANDLOCK_ACCESS_FS_REFER;
    }
    if tools.iter().any(|tool| tool == "bash") {
        access |= LANDLOCK_ACCESS_FS_EXECUTE;
    }
    access
}

#[cfg(target_os = "linux")]
fn landlock_abi_version() -> std::io::Result<i64> {
    // SAFETY: the syscall has no pointer arguments when querying the ABI.
    let version = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<LandlockRulesetAttr>(),
            0_usize,
            LANDLOCK_CREATE_RULESET_VERSION,
        )
    };
    if version < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(version)
    }
}

#[cfg(target_os = "linux")]
fn create_landlock_ruleset() -> anyhow::Result<fs::File> {
    let ruleset = LandlockRulesetAttr {
        handled_access_fs: LANDLOCK_ALL_FILESYSTEM_ACCESS,
    };
    // SAFETY: `ruleset` is a valid C-compatible structure for the syscall.
    let raw_fd = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            &ruleset as *const LandlockRulesetAttr,
            std::mem::size_of::<LandlockRulesetAttr>(),
            0_u32,
        )
    };
    if raw_fd < 0 {
        return Err(std::io::Error::last_os_error()).context("create Landlock ruleset");
    }
    let raw_fd = i32::try_from(raw_fd).context("Landlock ruleset file descriptor overflowed")?;
    // SAFETY: successful `landlock_create_ruleset` returns an owned file descriptor.
    let ruleset = unsafe { fs::File::from_raw_fd(raw_fd) };
    // SAFETY: the file descriptor is valid and owned by `ruleset`.
    let descriptor_flags = unsafe { libc::fcntl(ruleset.as_raw_fd(), libc::F_GETFD) };
    if descriptor_flags < 0 {
        return Err(std::io::Error::last_os_error()).context("read Landlock ruleset flags");
    }
    // SAFETY: the file descriptor is valid and owned by `ruleset`.
    if unsafe {
        libc::fcntl(
            ruleset.as_raw_fd(),
            libc::F_SETFD,
            descriptor_flags | libc::FD_CLOEXEC,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error()).context("protect Landlock ruleset descriptor");
    }
    Ok(ruleset)
}

#[cfg(target_os = "linux")]
fn add_landlock_path_rule(
    ruleset: &fs::File,
    path: &Path,
    kind: LandlockPathKind,
    access: u64,
) -> anyhow::Result<()> {
    let path =
        CString::new(path.as_os_str().as_bytes()).context("Landlock path contains a NUL byte")?;
    let mut flags = libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW;
    if kind == LandlockPathKind::Directory {
        flags |= libc::O_DIRECTORY;
    }
    // SAFETY: `path` is NUL-terminated and remains live for the syscall.
    let raw_fd = unsafe { libc::open(path.as_ptr(), flags) };
    if raw_fd < 0 {
        return Err(std::io::Error::last_os_error()).context("open Landlock rule path");
    }
    // SAFETY: successful `open` returns an owned file descriptor.
    let parent = unsafe { fs::File::from_raw_fd(raw_fd) };
    let rule = LandlockPathBeneathAttr {
        allowed_access: access,
        parent_fd: parent.as_raw_fd(),
    };
    // SAFETY: both file descriptors are valid and `rule` has the kernel ABI layout.
    let result = unsafe {
        libc::syscall(
            libc::SYS_landlock_add_rule,
            ruleset.as_raw_fd(),
            LANDLOCK_RULE_PATH_BENEATH,
            &rule as *const LandlockPathBeneathAttr,
            0_u32,
        )
    };
    if result < 0 {
        return Err(std::io::Error::last_os_error()).context("add Landlock path rule");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn restrict_landlock_child(ruleset_fd: RawFd) -> std::io::Result<()> {
    add_landlock_current_process_maps_rule(ruleset_fd)?;
    // SAFETY: this runs in `pre_exec` and invokes only raw syscalls on a valid FD.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `ruleset_fd` remains open across fork until this callback returns.
    if unsafe { libc::syscall(libc::SYS_landlock_restrict_self, ruleset_fd, 0_u32) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `ruleset_fd` is no longer needed after restriction is installed.
    if unsafe { libc::close(ruleset_fd) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn add_landlock_current_process_maps_rule(ruleset_fd: RawFd) -> std::io::Result<()> {
    // Bun reads its own memory map while initializing.  This must happen after
    // fork: resolving `/proc/self` in the Runtime parent would grant the wrong
    // process, whereas this rule applies only to the Pi child that will exec.
    static PROC_SELF_MAPS: &[u8] = b"/proc/self/maps\0";
    // SAFETY: the static path is NUL-terminated and this executes before the
    // child installs Landlock restrictions.
    let path_fd = unsafe {
        libc::open(
            PROC_SELF_MAPS.as_ptr().cast(),
            libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if path_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let rule = LandlockPathBeneathAttr {
        allowed_access: LANDLOCK_FILE_READ,
        parent_fd: path_fd,
    };
    // SAFETY: both descriptors are valid and `rule` has the kernel ABI layout.
    let result = unsafe {
        libc::syscall(
            libc::SYS_landlock_add_rule,
            ruleset_fd,
            LANDLOCK_RULE_PATH_BENEATH,
            &rule as *const LandlockPathBeneathAttr,
            0_u32,
        )
    };
    let rule_error = (result < 0).then(std::io::Error::last_os_error);
    // SAFETY: `path_fd` is owned by this child and no longer needed.
    let close_result = unsafe { libc::close(path_fd) };
    if let Some(error) = rule_error {
        return Err(error);
    }
    if close_result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

pub(super) struct PersistentPiRpcProcess {
    session_dir: PathBuf,
    session_file: PathBuf,
    native_session_id: String,
    next_request_id: u64,
    child: Option<std::process::Child>,
    stdin: Option<std::process::ChildStdin>,
    line_rx: Option<mpsc::Receiver<anyhow::Result<String>>>,
    stdout_reader: Option<std::thread::JoinHandle<()>>,
    stderr_reader: Option<std::thread::JoinHandle<Result<usize, std::io::Error>>>,
    skill_exec_broker: Option<SkillExecBroker>,
    cancellation: Arc<EngineCancellation>,
    timeout: Duration,
}

impl PersistentPiRpcProcess {
    pub(super) fn start(
        pi_bin: &str,
        run_env: &RunEnv,
        saved_session: Option<&Path>,
        tools: &[String],
        timeout: Duration,
        cancellation: Arc<EngineCancellation>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(!tools.is_empty(), "Pi tool allowlist must not be empty");
        let session_dir = run_env.engine_state_root.join(PI_SESSION_DIRECTORY);
        let agent_dir = pi_agent_directory(run_env);
        let pi_home = pi_home_directory(run_env);
        let pi_temp = pi_temp_directory(run_env);
        for (path, purpose) in [
            (&session_dir, "Pi Session directory"),
            (&agent_dir, "Pi Agent directory"),
            (&pi_home, "Pi Home directory"),
            (&pi_temp, "Pi temporary directory"),
        ] {
            prepare_private_directory(path, purpose)?;
        }
        let saved_session = saved_session
            .map(|path| validate_saved_session_path(&session_dir, path))
            .transpose()?;
        let skill_exec_enabled = tools.iter().any(|tool| tool == "skill_exec");
        materialize_skill_exec_extension(run_env, skill_exec_enabled)?;
        let skill_exec_broker = skill_exec_enabled
            .then(|| {
                SkillExecBroker::start(
                    run_env,
                    tools,
                    &run_env.hub_url,
                    run_env.maintenance_token_file.as_deref(),
                )
            })
            .transpose()
            .context("start Skill execution broker")?;

        #[cfg(target_os = "linux")]
        let filesystem_sandbox = PiFilesystemSandbox::prepare(
            pi_bin,
            run_env,
            &agent_dir,
            &session_dir,
            &pi_home,
            &pi_temp,
            tools,
        )
        .context("prepare Pi filesystem isolation")?;
        #[cfg(not(target_os = "linux"))]
        anyhow::bail!("Pi Runtime requires Linux Landlock Session isolation");

        let mut command = Command::new(pi_bin);
        command
            .env_clear()
            .arg("--mode")
            .arg("rpc")
            .arg("--session-dir")
            .arg(&session_dir)
            .arg("--no-extensions");
        let integration_extension = pi_integration_extension_path(run_env);
        if integration_extension.is_file() {
            command.arg("--extension").arg(&integration_extension);
        }
        let skill_exec_extension = pi_skill_exec_extension_path(run_env);
        if skill_exec_extension.is_file() {
            command.arg("--extension").arg(&skill_exec_extension);
        }
        command
            .arg("--no-themes")
            .arg("--no-prompt-templates")
            .arg("--tools")
            .arg(tools.join(","))
            .arg("--approve")
            .current_dir(&run_env.workdir)
            .env("HOME", &pi_home)
            .env("PI_CODING_AGENT_DIR", &agent_dir)
            .env("PI_OFFLINE", "1")
            .env("TMPDIR", &pi_temp)
            .env("TMP", &pi_temp)
            .env("TEMP", &pi_temp)
            .env("PATH", PI_PROCESS_PATH)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for secret in &run_env.secret_values {
            command.env(format!("AGENT_SECRET_{}", secret.name), &secret.value);
        }
        for file in &run_env.secret_files {
            let path = run_env.engine_state_root.join("secrets").join(&file.name);
            command.env(format!("AGENT_SECRET_FILE_{}", file.name), path);
        }
        if let Some(broker) = &skill_exec_broker {
            command
                .env("AGENT_HUB_SKILL_EXEC_SOCKET", broker.socket_path())
                .env("AGENT_HUB_SKILL_EXEC_TOKEN", broker.token());
        }
        if let Some(saved_session) = &saved_session {
            command.arg("--session").arg(saved_session);
        }
        for key in ["LANG", "LC_ALL"] {
            if let Some(value) = env::var_os(key) {
                command.env(key, value);
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        #[cfg(target_os = "linux")]
        {
            let ruleset_fd = filesystem_sandbox.ruleset_fd();
            // The closure runs between fork and exec, so it performs only raw
            // syscalls against the ruleset prepared by the parent.
            unsafe {
                command.pre_exec(move || restrict_landlock_child(ruleset_fd));
            }
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("spawn Pi RPC process: {pi_bin}"))?;
        #[cfg(target_os = "linux")]
        drop(filesystem_sandbox);
        cancellation.register_child(&child);
        let stdout = child.stdout.take().context("open Pi RPC stdout")?;
        let mut stderr = child.stderr.take().context("open Pi RPC stderr")?;
        let (line_tx, line_rx) =
            mpsc::sync_channel::<anyhow::Result<String>>(ENGINE_EVENT_QUEUE_CAPACITY);
        let stdout_reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line_tx
                    .send(line.context("read Pi RPC stdout line"))
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
        let stdin = child.stdin.take().context("open Pi RPC stdin")?;
        let mut process = Self {
            session_dir,
            session_file: PathBuf::new(),
            native_session_id: String::new(),
            next_request_id: 1,
            child: Some(child),
            stdin: Some(stdin),
            line_rx: Some(line_rx),
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            skill_exec_broker,
            cancellation,
            timeout,
        };
        if let Err(error) = process.initialize(saved_session.as_deref()) {
            process.shutdown();
            return Err(error);
        }
        Ok(process)
    }

    #[cfg(test)]
    pub(super) fn session_file(&self) -> &Path {
        &self.session_file
    }

    #[cfg(test)]
    pub(super) fn native_session_id(&self) -> &str {
        &self.native_session_id
    }

    pub(super) fn ensure_running(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.cancellation.is_cancelled(),
            "Pi RPC process is cancelled"
        );
        let child = self.child.as_mut().context("Pi RPC process is closed")?;
        if let Some(status) = child.try_wait().context("poll Pi RPC process")? {
            anyhow::bail!("Pi RPC process exited with status {status}");
        }
        Ok(())
    }

    pub(super) fn reload_resources(&mut self) -> anyhow::Result<()> {
        self.ensure_running()?;
        let started_at = Instant::now();
        let request_id = self.send_request(json!({ "type": "reload_resources" }))?;
        self.wait_for_response(&request_id, "reload_resources", started_at, None, None)?;
        Ok(())
    }

    fn initialize(&mut self, expected_session: Option<&Path>) -> anyhow::Result<()> {
        let request_id = self.send_request(json!({ "type": "get_state" }))?;
        let value = self.wait_for_response(&request_id, "get_state", Instant::now(), None, None)?;
        let state = value
            .get("data")
            .and_then(Value::as_object)
            .context("Pi get_state response is missing data")?;
        let native_session_id = state
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .context("Pi get_state response is missing sessionId")?;
        let session_file = state
            .get("sessionFile")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .context("Pi get_state response is missing sessionFile")?;
        let session_file =
            validate_reported_session_path(&self.session_dir, Path::new(session_file))?;
        if let Some(expected_session) = expected_session {
            anyhow::ensure!(
                same_path(&session_file, expected_session),
                "Pi opened a different Session file than requested"
            );
        }
        self.native_session_id = native_session_id.to_owned();
        self.session_file = session_file;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn execute(
        &mut self,
        claim: &ClaimRunResponse,
        event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
    ) -> anyhow::Result<EngineRunResult> {
        let result = self.execute_inner(claim, &event_tx, None, None);
        if result.is_err() {
            self.shutdown();
        }
        result
    }

    pub(super) fn execute_controlled(
        &mut self,
        claim: &ClaimRunResponse,
        event_tx: Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
        command_rx: &mpsc::Receiver<SessionSupervisorCommand>,
        deferred_commands: &mut VecDeque<SessionSupervisorCommand>,
    ) -> anyhow::Result<EngineRunResult> {
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
    ) -> anyhow::Result<EngineRunResult> {
        self.ensure_running()?;
        let started_at = Instant::now();
        let mut state = PiRunState::new(
            claim.run.id,
            self.native_session_id.clone(),
            integration_tool_name_map(claim.integration_context.as_ref())?,
        );
        self.configure_run(claim, &mut state, started_at, event_tx)?;

        let prompt = pi_prompt_text(claim)?;
        let prompt_request_id = self.send_request(json!({
            "type": "prompt",
            "message": prompt
        }))?;
        let mut pending = BTreeMap::from([(prompt_request_id, PendingPiRequest::Prompt)]);
        let mut accepted_interrupts: Vec<PendingInterruptResponse> = Vec::new();

        loop {
            if let Some(command_rx) = command_rx {
                loop {
                    match command_rx.try_recv() {
                        Ok(SessionSupervisorCommand::Steer {
                            expected_turn_id,
                            input,
                            response,
                        }) => {
                            if state.done
                                || !state.turn_active
                                || state.native_turn_id != expected_turn_id
                            {
                                let _ = response.send(Ok(SessionSteerOutcome::TurnEnded));
                                continue;
                            }
                            let request_id = self.send_request(json!({
                                "type": "steer",
                                "message": input.join("\n\n")
                            }))?;
                            pending.insert(
                                request_id,
                                PendingPiRequest::Steer(PendingSteerResponse { response }),
                            );
                        }
                        Ok(SessionSupervisorCommand::Interrupt {
                            expected_turn_id,
                            response,
                        }) => {
                            if state.done
                                || !state.turn_active
                                || state.native_turn_id != expected_turn_id
                            {
                                let _ = response.send(Ok(SessionInterruptOutcome::TurnEnded));
                                continue;
                            }
                            let request_id = self.send_request(json!({ "type": "abort" }))?;
                            pending.insert(
                                request_id,
                                PendingPiRequest::Interrupt(PendingInterruptResponse {
                                    expected_turn_id,
                                    response,
                                }),
                            );
                        }
                        Ok(command @ SessionSupervisorCommand::Execute { .. })
                        | Ok(command @ SessionSupervisorCommand::RefreshConfiguration { .. })
                        | Ok(command @ SessionSupervisorCommand::ReloadConfiguration { .. }) => {
                            deferred_commands
                                .as_deref_mut()
                                .context("controlled Pi execution has no deferred command queue")?
                                .push_back(command);
                        }
                        Ok(SessionSupervisorCommand::Stop) => {
                            anyhow::bail!("Session supervisor stopped during active Pi Turn");
                        }
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => {
                            anyhow::bail!("Session supervisor command channel disconnected");
                        }
                    }
                }
            }

            if state.done && !accepted_interrupts.is_empty() {
                for pending_interrupt in accepted_interrupts.drain(..) {
                    let outcome = if state.final_status == "interrupted"
                        && state.native_turn_id == pending_interrupt.expected_turn_id
                    {
                        SessionInterruptOutcome::Interrupted
                    } else {
                        SessionInterruptOutcome::TurnEnded
                    };
                    let _ = pending_interrupt.response.send(Ok(outcome));
                }
            }
            if state.done && pending.is_empty() && accepted_interrupts.is_empty() {
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
            let value: Value = serde_json::from_str(&line).context("parse Pi RPC JSON line")?;
            match value.get("type").and_then(Value::as_str) {
                Some("response") => {
                    let response_id = pi_response_id(&value)?;
                    let request = pending
                        .remove(response_id)
                        .context("Pi returned an unmatched RPC response")?;
                    match request {
                        PendingPiRequest::Prompt => {
                            validate_pi_response(&value, response_id, "prompt")?;
                        }
                        PendingPiRequest::Steer(pending_steer) => {
                            let outcome = if pi_response_succeeded(&value, response_id, "steer")? {
                                SessionSteerOutcome::Applied
                            } else {
                                SessionSteerOutcome::TurnEnded
                            };
                            let _ = pending_steer.response.send(Ok(outcome));
                        }
                        PendingPiRequest::Interrupt(pending_interrupt) => {
                            if pi_response_succeeded(&value, response_id, "abort")? {
                                accepted_interrupts.push(pending_interrupt);
                            } else {
                                let _ = pending_interrupt
                                    .response
                                    .send(Ok(SessionInterruptOutcome::TurnEnded));
                            }
                        }
                    }
                }
                Some(_) => {
                    state.handle_event(&value)?;
                    state.flush_events(event_tx, &self.cancellation)?;
                }
                None => anyhow::bail!("Pi RPC record is missing a type"),
            }
        }
        anyhow::ensure!(
            state.streamed_events > 0 || !state.events.is_empty(),
            "Pi RPC process produced no Run events"
        );
        Ok(state.finish())
    }

    fn configure_run(
        &mut self,
        claim: &ClaimRunResponse,
        state: &mut PiRunState,
        started_at: Instant,
        event_tx: &Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
    ) -> anyhow::Result<()> {
        let request_id = self.send_request(json!({ "type": "reload_models" }))?;
        self.wait_for_response(
            &request_id,
            "reload_models",
            started_at,
            Some(state),
            Some(event_tx),
        )?;
        let binding = model_binding(&claim.execution_configuration, "main")?;
        let provider = pi_model_provider_name(binding.id);
        let request_id = self.send_request(json!({
            "type": "set_model",
            "provider": provider,
            "modelId": binding.model_id
        }))?;
        self.wait_for_response(
            &request_id,
            "set_model",
            started_at,
            Some(state),
            Some(event_tx),
        )?;
        if let Some(level) = pi_thinking_level(binding.model_settings.reasoning_effort) {
            let request_id = self.send_request(json!({
                "type": "set_thinking_level",
                "level": level
            }))?;
            self.wait_for_response(
                &request_id,
                "set_thinking_level",
                started_at,
                Some(state),
                Some(event_tx),
            )?;
        }
        Ok(())
    }

    fn wait_for_response(
        &mut self,
        expected_id: &str,
        expected_command: &str,
        started_at: Instant,
        mut state: Option<&mut PiRunState>,
        event_tx: Option<&Option<tokio_mpsc::Sender<AppendRunEventRequest>>>,
    ) -> anyhow::Result<Value> {
        loop {
            let line = self.recv(started_at)?;
            let value: Value = serde_json::from_str(&line).context("parse Pi RPC JSON line")?;
            match value.get("type").and_then(Value::as_str) {
                Some("response") => {
                    validate_pi_response(&value, expected_id, expected_command)?;
                    return Ok(value);
                }
                Some(_) => {
                    let state = state
                        .as_deref_mut()
                        .context("Pi emitted an event outside an active Run")?;
                    state.handle_event(&value)?;
                    if let Some(event_tx) = event_tx {
                        state.flush_events(event_tx, &self.cancellation)?;
                    }
                }
                None => anyhow::bail!("Pi RPC record is missing a type"),
            }
        }
    }

    fn send_request(&mut self, mut value: Value) -> anyhow::Result<String> {
        let request_id = format!("agent-hub-{}", self.next_request_id);
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .context("Pi RPC request id overflow")?;
        value["id"] = json!(request_id);
        self.send(&value)?;
        Ok(request_id)
    }

    fn send(&mut self, value: &Value) -> anyhow::Result<()> {
        let stdin = self.stdin.as_mut().context("Pi RPC stdin is closed")?;
        serde_json::to_writer(&mut *stdin, value).context("serialize Pi RPC request")?;
        stdin.write_all(b"\n").context("write Pi RPC request")?;
        stdin.flush().context("flush Pi RPC request")
    }

    fn recv(&mut self, started_at: Instant) -> anyhow::Result<String> {
        loop {
            if let Some(line) = self.recv_once(started_at, Duration::from_millis(50))? {
                return Ok(line);
            }
        }
    }

    fn recv_once(&mut self, started_at: Instant, wait: Duration) -> anyhow::Result<Option<String>> {
        if self.cancellation.is_cancelled() {
            anyhow::bail!("Pi RPC process cancelled");
        }
        let line_rx = self.line_rx.as_ref().context("Pi RPC stdout is closed")?;
        match line_rx.recv_timeout(wait) {
            Ok(line) => return line.map(Some),
            Err(mpsc::RecvTimeoutError::Disconnected) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        let child = self.child.as_mut().context("Pi RPC process is closed")?;
        if let Some(status) = child.try_wait().context("poll Pi RPC process")? {
            anyhow::bail!("Pi RPC process exited early with status {status}");
        }
        if started_at.elapsed() > self.timeout {
            terminate_child_process_tree(child);
            return Err(PiRpcTimeout {
                timeout: self.timeout,
            }
            .into());
        }
        Ok(None)
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
        self.skill_exec_broker.take();
    }
}

impl Drop for PersistentPiRpcProcess {
    fn drop(&mut self) {
        self.shutdown();
    }
}

enum PendingPiRequest {
    Prompt,
    Steer(PendingSteerResponse),
    Interrupt(PendingInterruptResponse),
}

struct PiRunState {
    run_id: uuid::Uuid,
    native_session_id: String,
    native_turn_id: String,
    events: Vec<AppendRunEventRequest>,
    streamed_events: usize,
    final_status: String,
    done: bool,
    saw_agent_start: bool,
    saw_agent_end: bool,
    turn_started_emitted: bool,
    turn_active: bool,
    assistant_text: String,
    assistant_emitted_for_turn: bool,
    thinking_started: HashSet<u64>,
    thinking_ended: HashSet<u64>,
    tool_started: HashSet<String>,
    tool_ended: HashSet<String>,
    tool_outputs: BTreeMap<String, String>,
    integration_tool_names: BTreeMap<String, String>,
    integration_tool_requested: bool,
    retry_started: HashSet<u64>,
    retry_ended: HashSet<u64>,
    compaction_generation: u64,
    active_compaction: Option<String>,
}

impl PiRunState {
    fn new(
        run_id: uuid::Uuid,
        native_session_id: String,
        integration_tool_names: BTreeMap<String, String>,
    ) -> Self {
        Self {
            run_id,
            native_session_id,
            native_turn_id: run_id.to_string(),
            events: Vec::new(),
            streamed_events: 0,
            final_status: "completed".into(),
            done: false,
            saw_agent_start: false,
            saw_agent_end: false,
            turn_started_emitted: false,
            turn_active: false,
            assistant_text: String::new(),
            assistant_emitted_for_turn: false,
            thinking_started: HashSet::new(),
            thinking_ended: HashSet::new(),
            tool_started: HashSet::new(),
            tool_ended: HashSet::new(),
            tool_outputs: BTreeMap::new(),
            integration_tool_names,
            integration_tool_requested: false,
            retry_started: HashSet::new(),
            retry_ended: HashSet::new(),
            compaction_generation: 0,
            active_compaction: None,
        }
    }

    fn handle_event(&mut self, value: &Value) -> anyhow::Result<()> {
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .context("Pi event is missing a type")?;
        match event_type {
            "agent_start" => {
                self.saw_agent_start = true;
            }
            "turn_start" => {
                anyhow::ensure!(self.saw_agent_start, "Pi Turn started before agent_start");
                if self.turn_active {
                    return Ok(());
                }
                self.turn_active = true;
                self.assistant_text.clear();
                self.assistant_emitted_for_turn = false;
                self.thinking_started.clear();
                self.thinking_ended.clear();
                self.push_turn_started();
            }
            "message_start" => {}
            "message_update" => self.handle_message_update(value)?,
            "message_end" => {
                if value
                    .get("message")
                    .and_then(|message| message.get("role"))
                    .and_then(Value::as_str)
                    == Some("assistant")
                {
                    let message = value
                        .get("message")
                        .context("Pi message_end has no message")?;
                    self.push_assistant_message(message);
                }
            }
            "tool_execution_start" => self.handle_tool_start(value)?,
            "tool_execution_update" => self.handle_tool_update(value)?,
            "tool_execution_end" => self.handle_tool_end(value)?,
            "turn_end" => self.handle_turn_end(value)?,
            "agent_end" => {
                self.saw_agent_end = true;
            }
            "agent_settled" => {
                anyhow::ensure!(self.saw_agent_end, "Pi settled before agent_end");
                if self.done {
                    return Ok(());
                }
                self.done = true;
                if self.final_status != "completed" {
                    self.events.push(AppendRunEventRequest {
                        event_type: "status".into(),
                        role: None,
                        content: Some(self.final_status.clone()),
                        payload: json!({ "kind": "pi_terminal" }),
                        waiting_tool: None,
                    });
                }
            }
            "queue_update"
            | "entry_appended"
            | "session_info_changed"
            | "thinking_level_changed" => {}
            "compaction_start" => self.handle_compaction_start(value)?,
            "compaction_end" => self.handle_compaction_end(value)?,
            "auto_retry_start" => self.handle_retry_start(value)?,
            "auto_retry_end" => self.handle_retry_end(value)?,
            "summarization_retry_scheduled"
            | "summarization_retry_attempt_start"
            | "summarization_retry_finished" => self.handle_summarization_retry(value)?,
            _ => anyhow::bail!("Pi RPC emitted an unsupported event type: {event_type}"),
        }
        Ok(())
    }

    fn handle_message_update(&mut self, value: &Value) -> anyhow::Result<()> {
        anyhow::ensure!(self.turn_active, "Pi message update arrived outside a Turn");
        let event = value
            .get("assistantMessageEvent")
            .and_then(Value::as_object)
            .context("Pi message_update is missing assistantMessageEvent")?;
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .context("Pi assistant message event is missing a type")?;
        match event_type {
            "start" | "text_start" | "text_end" | "toolcall_start" | "toolcall_delta"
            | "toolcall_end" | "done" => {}
            "text_delta" => {
                let delta = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .context("Pi text_delta is missing delta")?;
                self.assistant_text.push_str(delta);
                self.events.push(AppendRunEventRequest {
                    event_type: "message_delta".into(),
                    role: Some("assistant".into()),
                    content: Some(delta.to_owned()),
                    payload: json!({ "stream": true, "source": "pi" }),
                    waiting_tool: None,
                });
            }
            "thinking_start" => {
                let index = pi_content_index(event)?;
                if self.thinking_started.insert(index) {
                    self.events.push(pi_reasoning_event(index, "started", None));
                }
            }
            "thinking_delta" => {
                let index = pi_content_index(event)?;
                let delta = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .context("Pi thinking_delta is missing delta")?;
                if self.thinking_started.insert(index) {
                    self.events.push(pi_reasoning_event(index, "started", None));
                }
                self.events
                    .push(pi_reasoning_event(index, "summary_delta", Some(delta)));
            }
            "thinking_end" => {
                let index = pi_content_index(event)?;
                if self.thinking_ended.insert(index) {
                    let content = event.get("content").and_then(Value::as_str);
                    self.events
                        .push(pi_reasoning_event(index, "completed", content));
                }
            }
            "error" => {
                self.final_status = match event.get("reason").and_then(Value::as_str) {
                    Some("aborted") => "interrupted".into(),
                    Some("error") => "failed".into(),
                    _ => anyhow::bail!("Pi assistant error has an unsupported reason"),
                };
            }
            _ => anyhow::bail!("Pi emitted an unsupported assistant event: {event_type}"),
        }
        Ok(())
    }

    fn handle_turn_end(&mut self, value: &Value) -> anyhow::Result<()> {
        if !self.turn_active {
            return Ok(());
        }
        let message = value
            .get("message")
            .and_then(Value::as_object)
            .context("Pi turn_end is missing message")?;
        anyhow::ensure!(
            message.get("role").and_then(Value::as_str) == Some("assistant"),
            "Pi turn_end message is not an assistant message"
        );
        self.push_assistant_message(&Value::Object(message.clone()));
        if let Some(usage) = message.get("usage") {
            self.events.push(AppendRunEventRequest {
                event_type: "usage".into(),
                role: None,
                content: None,
                payload: usage.clone(),
                waiting_tool: None,
            });
        }
        if let Some(stop_reason) = message.get("stopReason").and_then(Value::as_str) {
            self.final_status = match stop_reason {
                "aborted" => "interrupted".into(),
                "error" => "failed".into(),
                "toolUse" if self.integration_tool_requested => "waiting_tool".into(),
                "stop" | "length" | "toolUse" => "completed".into(),
                _ => anyhow::bail!("Pi turn_end has an unsupported stop reason"),
            };
        }
        self.turn_active = false;
        Ok(())
    }

    fn push_turn_started(&mut self) {
        if self.turn_started_emitted {
            return;
        }
        self.events.push(AppendRunEventRequest {
            event_type: "turn_started".into(),
            role: None,
            content: None,
            payload: json!({
                "native_session_id": self.native_session_id,
                "native_turn_id": self.native_turn_id
            }),
            waiting_tool: None,
        });
        self.turn_started_emitted = true;
    }

    fn push_assistant_message(&mut self, message: &Value) {
        if self.assistant_emitted_for_turn {
            return;
        }
        let content = pi_message_text(message);
        let content = if content.is_empty() {
            self.assistant_text.clone()
        } else {
            content
        };
        if content.is_empty() {
            return;
        }
        self.events.push(AppendRunEventRequest {
            event_type: "message".into(),
            role: Some("assistant".into()),
            content: Some(content),
            payload: json!({
                "source": "pi",
                "stop_reason": message.get("stopReason").cloned().unwrap_or(Value::Null)
            }),
            waiting_tool: None,
        });
        self.assistant_emitted_for_turn = true;
    }

    fn handle_tool_start(&mut self, value: &Value) -> anyhow::Result<()> {
        let (tool_call_id, tool_name) = pi_tool_identity(value)?;
        if !self.tool_started.insert(tool_call_id.to_owned()) {
            return Ok(());
        }
        let args = value.get("args").cloned().unwrap_or_else(|| json!({}));
        if let Some(external_name) = self.integration_tool_names.get(tool_name).cloned() {
            self.integration_tool_requested = true;
            self.events.push(pi_integration_tool_request(
                self.run_id,
                tool_call_id,
                &external_name,
                args,
            ));
            return Ok(());
        }
        self.events.push(pi_tool_event(
            tool_call_id,
            tool_name,
            "started",
            &args,
            None,
            None,
        ));
        Ok(())
    }

    fn handle_tool_update(&mut self, value: &Value) -> anyhow::Result<()> {
        let (tool_call_id, tool_name) = pi_tool_identity(value)?;
        anyhow::ensure!(
            self.tool_started.contains(tool_call_id),
            "Pi tool update arrived before tool start"
        );
        anyhow::ensure!(
            !self.tool_ended.contains(tool_call_id),
            "Pi tool update arrived after tool end"
        );
        if self.integration_tool_names.contains_key(tool_name) {
            return Ok(());
        }
        let output = pi_result_text(value.get("partialResult"));
        let previous = self
            .tool_outputs
            .entry(tool_call_id.to_owned())
            .or_default();
        let delta = output
            .strip_prefix(previous.as_str())
            .unwrap_or(output.as_str())
            .to_owned();
        *previous = output;
        if delta.is_empty() {
            return Ok(());
        }
        let args = value.get("args").cloned().unwrap_or_else(|| json!({}));
        self.events.push(pi_tool_event(
            tool_call_id,
            tool_name,
            "output_delta",
            &args,
            Some(&delta),
            None,
        ));
        Ok(())
    }

    fn handle_tool_end(&mut self, value: &Value) -> anyhow::Result<()> {
        let (tool_call_id, tool_name) = pi_tool_identity(value)?;
        anyhow::ensure!(
            self.tool_started.contains(tool_call_id),
            "Pi tool end arrived before tool start"
        );
        if !self.tool_ended.insert(tool_call_id.to_owned()) {
            return Ok(());
        }
        if self.integration_tool_names.contains_key(tool_name) {
            return Ok(());
        }
        let output = pi_result_text(value.get("result"));
        let is_error = value
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let args = value.get("args").cloned().unwrap_or_else(|| json!({}));
        self.events.push(pi_tool_event(
            tool_call_id,
            tool_name,
            "completed",
            &args,
            (!output.is_empty()).then_some(output.as_str()),
            Some(!is_error),
        ));
        Ok(())
    }

    fn handle_retry_start(&mut self, value: &Value) -> anyhow::Result<()> {
        let attempt = value
            .get("attempt")
            .and_then(Value::as_u64)
            .context("Pi auto_retry_start is missing attempt")?;
        if !self.retry_started.insert(attempt) {
            return Ok(());
        }
        self.events.push(AppendRunEventRequest {
            event_type: "item".into(),
            role: Some("assistant".into()),
            content: None,
            payload: json!({
                "item_id": format!("pi-retry-{attempt}"),
                "item_type": "retry",
                "phase": "started",
                "attempt": attempt,
                "max_attempts": value.get("maxAttempts").cloned().unwrap_or(Value::Null),
                "delay_ms": value.get("delayMs").cloned().unwrap_or(Value::Null)
            }),
            waiting_tool: None,
        });
        Ok(())
    }

    fn handle_retry_end(&mut self, value: &Value) -> anyhow::Result<()> {
        let attempt = value
            .get("attempt")
            .and_then(Value::as_u64)
            .context("Pi auto_retry_end is missing attempt")?;
        if !self.retry_ended.insert(attempt) {
            return Ok(());
        }
        self.events.push(AppendRunEventRequest {
            event_type: "item".into(),
            role: Some("assistant".into()),
            content: None,
            payload: json!({
                "item_id": format!("pi-retry-{attempt}"),
                "item_type": "retry",
                "phase": "completed",
                "attempt": attempt,
                "success": value.get("success").cloned().unwrap_or(Value::Bool(false))
            }),
            waiting_tool: None,
        });
        Ok(())
    }

    fn handle_compaction_start(&mut self, value: &Value) -> anyhow::Result<()> {
        if self.active_compaction.is_some() {
            return Ok(());
        }
        self.compaction_generation = self.compaction_generation.saturating_add(1);
        let item_id = format!("pi-compaction-{}", self.compaction_generation);
        self.active_compaction = Some(item_id.clone());
        self.events.push(AppendRunEventRequest {
            event_type: "item".into(),
            role: Some("assistant".into()),
            content: None,
            payload: json!({
                "item_id": item_id,
                "item_type": "contextCompaction",
                "phase": "started",
                "reason": value.get("reason").cloned().unwrap_or(Value::Null)
            }),
            waiting_tool: None,
        });
        Ok(())
    }

    fn handle_compaction_end(&mut self, value: &Value) -> anyhow::Result<()> {
        let Some(item_id) = self.active_compaction.take() else {
            return Ok(());
        };
        self.events.push(AppendRunEventRequest {
            event_type: "item".into(),
            role: Some("assistant".into()),
            content: None,
            payload: json!({
                "item_id": item_id,
                "item_type": "contextCompaction",
                "phase": "completed",
                "reason": value.get("reason").cloned().unwrap_or(Value::Null),
                "aborted": value.get("aborted").cloned().unwrap_or(Value::Bool(false)),
                "will_retry": value.get("willRetry").cloned().unwrap_or(Value::Bool(false))
            }),
            waiting_tool: None,
        });
        Ok(())
    }

    fn handle_summarization_retry(&mut self, value: &Value) -> anyhow::Result<()> {
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        self.events.push(AppendRunEventRequest {
            event_type: "item".into(),
            role: Some("assistant".into()),
            content: None,
            payload: json!({
                "item_id": format!("pi-summarization-{}", self.run_id),
                "item_type": "contextCompaction",
                "phase": event_type,
                "attempt": value.get("attempt").cloned().unwrap_or(Value::Null),
                "max_attempts": value.get("maxAttempts").cloned().unwrap_or(Value::Null),
                "delay_ms": value.get("delayMs").cloned().unwrap_or(Value::Null),
                "source": value.get("source").cloned().unwrap_or(Value::Null),
                "reason": value.get("reason").cloned().unwrap_or(Value::Null)
            }),
            waiting_tool: None,
        });
        Ok(())
    }

    fn flush_events(
        &mut self,
        event_tx: &Option<tokio_mpsc::Sender<AppendRunEventRequest>>,
        cancellation: &EngineCancellation,
    ) -> anyhow::Result<()> {
        let Some(event_tx) = event_tx else {
            return Ok(());
        };
        for event in self.events.drain(..) {
            send_engine_event_with_backpressure(event_tx, event, cancellation)?;
            self.streamed_events += 1;
        }
        Ok(())
    }

    fn finish(self) -> EngineRunResult {
        EngineRunResult {
            events: self.events,
            final_status: self.final_status,
            native_session_id: Some(self.native_session_id),
            native_turn_id: Some(self.native_turn_id),
        }
    }
}

pub(super) fn pi_tool_allowlist(agent: &AgentDto) -> Vec<String> {
    let enabled = agent
        .tool_allowlist
        .iter()
        .map(|tool| tool.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut tools = ["read", "grep", "find", "ls"]
        .into_iter()
        .filter(|tool| enabled.contains(tool))
        .collect::<Vec<_>>();
    let mode = agent
        .sandbox_policy
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("read-only");
    let writable = matches!(
        mode,
        "workspace-write" | "workspaceWrite" | "danger-full-access" | "dangerFullAccess"
    );
    if writable {
        tools.extend(
            ["edit", "write"]
                .into_iter()
                .filter(|tool| enabled.contains(tool)),
        );
        if agent
            .sandbox_policy
            .get("network_access")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            && enabled.contains("bash")
        {
            tools.push("bash");
        }
    }
    tools.into_iter().map(str::to_owned).collect()
}

pub(super) fn pi_tool_allowlist_for_claim(claim: &ClaimRunResponse) -> anyhow::Result<Vec<String>> {
    let mut effective_agent = claim.agent.clone();
    effective_agent.tool_allowlist = claim.execution_configuration.tool_allowlist.clone();
    effective_agent.sandbox_policy = claim.execution_configuration.sandbox_policy.clone();
    let mut tools = pi_tool_allowlist(&effective_agent);
    let skill_exec_enabled = claim
        .execution_configuration
        .tool_allowlist
        .iter()
        .any(|tool| tool == "skill_exec")
        && claim.execution_configuration.skills.iter().any(|skill| {
            skill
                .package
                .as_ref()
                .is_some_and(|package| package.files.iter().any(|file| file.executable))
        });
    if skill_exec_enabled && !tools.iter().any(|tool| tool == "skill_exec") {
        tools.push("skill_exec".into());
    }
    if claim
        .execution_configuration
        .tool_allowlist
        .iter()
        .any(|tool| tool == "integration")
    {
        for name in integration_tool_name_map(claim.integration_context.as_ref())?.into_keys() {
            if !tools.contains(&name) {
                tools.push(name);
            }
        }
    }
    Ok(tools)
}

pub(super) fn discover_session_file(
    pi_home: &Path,
    expected_native_session_id: &str,
) -> anyhow::Result<PathBuf> {
    anyhow::ensure!(
        !expected_native_session_id.trim().is_empty(),
        "Pi Session id must not be empty"
    );
    let session_dir = pi_home.join(PI_SESSION_DIRECTORY);
    let mut matched = None;
    for entry in fs::read_dir(&session_dir).context("read Pi Session directory")? {
        let entry = entry.context("read Pi Session directory entry")?;
        let file_type = entry.file_type().context("inspect Pi Session entry")?;
        if !file_type.is_file()
            || entry.path().extension().and_then(|value| value.to_str()) != Some("jsonl")
        {
            continue;
        }
        let file = fs::File::open(entry.path()).context("open Pi Session candidate")?;
        let first_line = BufReader::new(file)
            .lines()
            .next()
            .transpose()
            .context("read Pi Session header")?
            .context("Pi Session candidate is empty")?;
        let header: Value = serde_json::from_str(&first_line).context("parse Pi Session header")?;
        if header.get("type").and_then(Value::as_str) != Some("session")
            || header.get("id").and_then(Value::as_str) != Some(expected_native_session_id)
        {
            continue;
        }
        anyhow::ensure!(
            matched.is_none(),
            "multiple Pi Session files have the same id"
        );
        matched = Some(entry.path());
    }
    matched.context("Pi Session recovery file was not found")
}

pub(super) fn pi_prompt_text(claim: &ClaimRunResponse) -> anyhow::Result<String> {
    let mut messages = claim
        .session_context
        .as_ref()
        .map(|context| context.messages.iter().collect::<Vec<_>>())
        .unwrap_or_default();
    messages.sort_by_key(|message| message.sequence);
    let prompt = messages
        .into_iter()
        .filter_map(|message| message.content.as_deref())
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    let mut prompt = if prompt.trim().is_empty() {
        claim.run.initial_message.trim().to_owned()
    } else {
        prompt
    };
    anyhow::ensure!(!prompt.is_empty(), "Pi prompt must not be empty");
    if let Some(context) = &claim.integration_context {
        let envelope = json!({
            "message": prompt,
            "attachments": context.attachments,
            "tool_result": context.tool_result,
            "tool_results": context.tool_results,
            "external_user": context.external_user
        });
        prompt.push_str("\n\n");
        prompt.push_str(PI_INTEGRATION_CONTEXT_LABEL);
        prompt.push('\n');
        prompt.push_str(&serde_json::to_string(&envelope)?);
    }
    Ok(prompt)
}

fn validate_saved_session_path(session_dir: &Path, path: &Path) -> anyhow::Result<PathBuf> {
    anyhow::ensure!(path.is_file(), "saved Pi Session file is unavailable");
    validate_reported_session_path(session_dir, path)
}

fn validate_reported_session_path(session_dir: &Path, path: &Path) -> anyhow::Result<PathBuf> {
    let path = if path.is_absolute() || path.starts_with(session_dir) {
        path.to_path_buf()
    } else {
        session_dir.join(path)
    };
    let parent = path.parent().context("Pi Session file has no parent")?;
    anyhow::ensure!(
        same_path(parent, session_dir),
        "Pi Session file is outside its isolated Session directory"
    );
    anyhow::ensure!(
        path.extension().and_then(|extension| extension.to_str()) == Some("jsonl"),
        "Pi Session file must be JSONL"
    );
    Ok(path)
}

fn same_path(left: &Path, right: &Path) -> bool {
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

fn pi_response_id(value: &Value) -> anyhow::Result<&str> {
    value
        .get("id")
        .and_then(Value::as_str)
        .context("Pi RPC response is missing a string id")
}

fn pi_response_succeeded(
    value: &Value,
    expected_id: &str,
    expected_command: &str,
) -> anyhow::Result<bool> {
    anyhow::ensure!(
        value.get("type").and_then(Value::as_str) == Some("response"),
        "Pi RPC response has an invalid type"
    );
    anyhow::ensure!(
        pi_response_id(value)? == expected_id,
        "Pi RPC response id does not match its request"
    );
    anyhow::ensure!(
        value.get("command").and_then(Value::as_str) == Some(expected_command),
        "Pi RPC response command does not match its request"
    );
    value
        .get("success")
        .and_then(Value::as_bool)
        .context("Pi RPC response is missing success")
}

fn validate_pi_response(
    value: &Value,
    expected_id: &str,
    expected_command: &str,
) -> anyhow::Result<()> {
    if !pi_response_succeeded(value, expected_id, expected_command)? {
        #[cfg(test)]
        anyhow::bail!(
            "Pi RPC command {expected_command} failed: {}",
            value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unspecified error")
        );
        #[cfg(not(test))]
        anyhow::bail!("Pi RPC command {expected_command} failed");
    }
    Ok(())
}

fn pi_content_index(event: &serde_json::Map<String, Value>) -> anyhow::Result<u64> {
    event
        .get("contentIndex")
        .and_then(Value::as_u64)
        .context("Pi assistant event is missing contentIndex")
}

fn pi_reasoning_event(index: u64, phase: &str, content: Option<&str>) -> AppendRunEventRequest {
    let mut payload = json!({
        "item_id": format!("pi-thinking-{index}"),
        "item_type": "reasoning",
        "phase": phase
    });
    if let Some(content) = content {
        payload["summary"] = if phase == "completed" {
            json!([content])
        } else {
            json!(content)
        };
    }
    AppendRunEventRequest {
        event_type: "item".into(),
        role: Some("assistant".into()),
        content: None,
        payload,
        waiting_tool: None,
    }
}

fn pi_message_text(message: &Value) -> String {
    message
        .get("content")
        .and_then(Value::as_array)
        .map(|parts| {
            parts
                .iter()
                .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

fn pi_tool_identity(value: &Value) -> anyhow::Result<(&str, &str)> {
    let tool_call_id = value
        .get("toolCallId")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("Pi tool event is missing toolCallId")?;
    let tool_name = value
        .get("toolName")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("Pi tool event is missing toolName")?;
    Ok((tool_call_id, tool_name))
}

fn pi_tool_event(
    tool_call_id: &str,
    tool_name: &str,
    phase: &str,
    args: &Value,
    output: Option<&str>,
    success: Option<bool>,
) -> AppendRunEventRequest {
    let is_command = tool_name == "bash";
    let mut payload = json!({
        "item_id": tool_call_id,
        "item_type": if is_command { "commandExecution" } else { "dynamicToolCall" },
        "phase": phase,
        "tool": tool_name,
        "arguments": args
    });
    if is_command {
        payload["command"] = args
            .get("command")
            .cloned()
            .unwrap_or_else(|| json!(args.to_string()));
    }
    if let Some(output) = output {
        payload["output"] = json!(output);
    }
    if let Some(success) = success {
        payload["success"] = json!(success);
        payload["status"] = json!(if success { "completed" } else { "failed" });
    }
    AppendRunEventRequest {
        event_type: "item".into(),
        role: Some("assistant".into()),
        content: None,
        payload,
        waiting_tool: None,
    }
}

fn pi_integration_tool_request(
    run_id: uuid::Uuid,
    tool_call_id: &str,
    tool_name: &str,
    arguments: Value,
) -> AppendRunEventRequest {
    let source_id = tool_call_id
        .rsplit_once('|')
        .filter(|(_, item_id)| item_id.starts_with("fc_"))
        .map_or(tool_call_id, |(call_id, _)| call_id);
    let tool_request_id =
        stable_tool_request_uuid(run_id, tool_name, Some(source_id), &arguments).to_string();
    AppendRunEventRequest {
        event_type: "tool_request".into(),
        role: Some("assistant".into()),
        content: Some(format!("Pi requested {tool_name} tool")),
        payload: json!({
            "tool_request_id": tool_request_id,
            "source_id": source_id,
            "tool_name": tool_name,
            "arguments": arguments
        }),
        waiting_tool: None,
    }
}

fn pi_result_text(result: Option<&Value>) -> String {
    let Some(result) = result else {
        return String::new();
    };
    if let Some(text) = result.as_str() {
        return text.to_owned();
    }
    result
        .get("content")
        .and_then(Value::as_array)
        .map(|parts| {
            parts
                .iter()
                .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::{
        os::unix::process::CommandExt,
        process::{Command, Output, Stdio},
    };

    use super::*;
    use crate::RunEnv;

    struct IsolatedRunEnv {
        run_env: RunEnv,
        agent_dir: PathBuf,
        session_dir: PathBuf,
        pi_home: PathBuf,
        pi_temp: PathBuf,
    }

    fn isolated_run_env(root: &Path, name: &str) -> IsolatedRunEnv {
        let session_root = root.join(name);
        let run_env = RunEnv {
            workdir: session_root.join("workspace"),
            engine_state_root: session_root.join("engine-state"),
            hub_url: "http://127.0.0.1:8080".into(),
            maintenance_token_file: None,
            secret_values: Vec::new(),
            secret_files: Vec::new(),
        };
        let agent_dir = pi_agent_directory(&run_env);
        let session_dir = run_env.engine_state_root.join(PI_SESSION_DIRECTORY);
        let pi_home = pi_home_directory(&run_env);
        let pi_temp = pi_temp_directory(&run_env);
        for path in [
            &run_env.workdir,
            &agent_dir,
            &session_dir,
            &pi_home,
            &pi_temp,
        ] {
            prepare_private_directory(path, "Landlock test directory").unwrap();
        }
        IsolatedRunEnv {
            run_env,
            agent_dir,
            session_dir,
            pi_home,
            pi_temp,
        }
    }

    fn run_sandboxed_shell(
        fixture: &IsolatedRunEnv,
        tools: &[&str],
        script: &str,
        args: &[&Path],
    ) -> Output {
        let tools = tools
            .iter()
            .map(|tool| (*tool).to_owned())
            .collect::<Vec<_>>();
        let sandbox = PiFilesystemSandbox::prepare(
            "/bin/sh",
            &fixture.run_env,
            &fixture.agent_dir,
            &fixture.session_dir,
            &fixture.pi_home,
            &fixture.pi_temp,
            &tools,
        )
        .unwrap();
        let ruleset_fd = sandbox.ruleset_fd();
        let mut command = Command::new("/bin/sh");
        command
            .arg("-ceu")
            .arg(script)
            .arg("sh")
            .args(args)
            .current_dir(&fixture.run_env.workdir)
            .env_clear()
            .env("HOME", &fixture.pi_home)
            .env("PI_CODING_AGENT_DIR", &fixture.agent_dir)
            .env("PI_OFFLINE", "1")
            .env("TMPDIR", &fixture.pi_temp)
            .env("TMP", &fixture.pi_temp)
            .env("TEMP", &fixture.pi_temp)
            .env("PATH", PI_PROCESS_PATH)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // The same pre-exec hook used by the persistent Pi process.
        unsafe {
            command.pre_exec(move || restrict_landlock_child(ruleset_fd));
        }
        let output = command.output().unwrap();
        drop(sandbox);
        output
    }

    #[test]
    fn landlock_blocks_unmanaged_and_sibling_session_reads_but_keeps_own_pi_state_writable() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = isolated_run_env(temp.path(), "first");
        let own_workspace_file = fixture.run_env.workdir.join("allowed.txt");
        let unmanaged_state = fixture
            .run_env
            .engine_state_root
            .join("unmanaged-state.json");
        std::fs::write(&own_workspace_file, "own-workspace\n").unwrap();
        std::fs::write(&unmanaged_state, "ENGINE_SECRET=unmanaged-secret\n").unwrap();

        let sibling = isolated_run_env(temp.path(), "second");
        let sibling_workspace_file = sibling.run_env.workdir.join("secret.txt");
        let sibling_models_file = sibling.agent_dir.join("models.json");
        std::fs::write(&sibling_workspace_file, "sibling-workspace-secret\n").unwrap();
        std::fs::write(&sibling_models_file, "sibling-model-secret\n").unwrap();

        let agent_state = fixture.agent_dir.join("state.txt");
        let session_state = fixture.session_dir.join("state.jsonl");
        let temp_state = fixture.pi_temp.join("state.txt");
        let denied_workspace_write = fixture.run_env.workdir.join("must-not-write.txt");
        let output = run_sandboxed_shell(
            &fixture,
            &["read"],
            r#"
IFS= read -r own < "$1"
[ "$own" = "own-workspace" ]
printf agent-state > "$2"
printf session-state > "$3"
printf temp-state > "$4"
if /bin/sh -c 'IFS= read -r _ < "$1"' sh "$5"; then exit 11; fi
if /bin/sh -c 'IFS= read -r _ < "$1"' sh "$6"; then exit 12; fi
if /bin/sh -c 'IFS= read -r _ < "$1"' sh "$7"; then exit 13; fi
if /bin/sh -c ': > "$1"' sh "$8"; then exit 14; fi
"#,
            &[
                &own_workspace_file,
                &agent_state,
                &session_state,
                &temp_state,
                &unmanaged_state,
                &sibling_workspace_file,
                &sibling_models_file,
                &denied_workspace_write,
            ],
        );

        assert!(
            output.status.success(),
            "sandbox probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(std::fs::read_to_string(agent_state).unwrap(), "agent-state");
        assert_eq!(
            std::fs::read_to_string(session_state).unwrap(),
            "session-state"
        );
        assert_eq!(std::fs::read_to_string(temp_state).unwrap(), "temp-state");
        assert!(!denied_workspace_write.exists());
    }

    #[test]
    fn landlock_allows_workspace_rename_but_rejects_sibling_destination() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = isolated_run_env(temp.path(), "first");
        let _ = isolated_run_env(temp.path(), "second");
        let own_source = fixture.run_env.workdir.join("before.txt");
        let own_destination = fixture.run_env.workdir.join("after.txt");
        let sibling_destination = temp
            .path()
            .join("second")
            .join("workspace")
            .join("forbidden.txt");

        let output = run_sandboxed_shell(
            &fixture,
            &["bash"],
            r#"
printf own > "$1"
mv "$1" "$2"
test -f "$2"
if mv "$2" "$3"; then exit 21; fi
test -f "$2"
"#,
            &[&own_source, &own_destination, &sibling_destination],
        );

        assert!(
            output.status.success(),
            "sandbox rename probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(std::fs::read_to_string(own_destination).unwrap(), "own");
        assert!(!sibling_destination.exists());
    }

    #[test]
    fn landlock_allows_only_the_childs_own_proc_maps() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = isolated_run_env(temp.path(), "first");

        let output = run_sandboxed_shell(
            &fixture,
            &["read"],
            r#"
IFS= read -r own_map < /proc/self/maps
[ -n "$own_map" ]
if [ "$$" -ne 1 ] && IFS= read -r _ < /proc/1/maps; then exit 31; fi
"#,
            &[],
        );

        assert!(
            output.status.success(),
            "sandbox proc probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
