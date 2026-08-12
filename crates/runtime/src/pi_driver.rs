use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    env, fmt, fs,
    io::{BufRead, BufReader, Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
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
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc as tokio_mpsc;
use tracing::warn;

use super::{
    model_binding, pi_model_provider_name, pi_thinking_level, send_engine_event_with_backpressure,
    stable_tool_request_uuid, terminate_child_process_tree, EngineCancellation, EngineRunResult,
    PendingInterruptResponse, PendingSteerResponse, RunEnv, SessionInterruptOutcome,
    SessionSteerOutcome, SessionSupervisorCommand, ENGINE_EVENT_QUEUE_CAPACITY,
};

mod skill_exec;

use skill_exec::{SkillExecBroker, SKILL_EXEC_EXTENSION_SOURCE};

const PI_SESSION_DIRECTORY: &str = "sessions";
const PI_WORKSPACE_MOUNT: &str = "/workspace";
const PI_AGENT_STATE_MOUNT: &str = "/agent-state";
const PI_TMP_MOUNT: &str = "/tmp";
#[cfg_attr(test, allow(dead_code))]
const PI_SANDBOX_UID: u32 = 10001;
#[cfg_attr(test, allow(dead_code))]
const PI_SANDBOX_GID: u32 = 10001;
const PI_AGENT_DIRECTORY: &str = ".pi/agent";
const PI_HOME_DIRECTORY: &str = ".pi/home";
const PI_TEMP_DIRECTORY: &str = ".pi/tmp";
const PI_PROCESS_PATH: &str = "/usr/bin:/bin";
const PI_INTEGRATION_EXTENSION_FILE: &str = "agent-hub-integration-tools.mjs";
const PI_INTEGRATION_TOOLS_FILE: &str = "agent-hub-integration-tools.json";
const PI_SKILL_EXEC_EXTENSION_FILE: &str = "agent-hub-skill-exec.mjs";
const PI_VISION_EXTENSION_FILE: &str = "agent-hub-vision.mjs";
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
    "vision_analyze",
];
const MAX_VISION_IMAGE_BYTES: u64 = 100 * 1024 * 1024;
const MAX_TOOL_OUTPUT_EVENT_BYTES: usize = 32 * 1024;
const MAX_VISION_REQUEST_BYTES: usize = 128 * 1024;
const VISION_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);
const VISION_HTTP_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_VISION_PROMPT: &str = "请描述这张图片的内容";
const VISION_PROXY_API_KEY_PLACEHOLDER: &str = "agent-hub-local-proxy";

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

const PI_TOOL_RESULT_EXTENSION_SOURCE: &str = r#"
import { createConnection } from "node:net";

const portValue = process.env.AGENT_HUB_TOOL_RESULT_PORT;
const token = process.env.AGENT_HUB_TOOL_RESULT_TOKEN;
if (!portValue || !token) {
  throw new Error("Agent Hub tool result broker is not configured");
}

function callBroker(params, signal) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let buffer = "";
    const socket = createConnection({ host: "127.0.0.1", port: Number(portValue) });
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      signal?.removeEventListener("abort", onAbort);
      socket.destroy();
      callback(value);
    };
    const onAbort = () => finish(reject, new Error("Tool result read aborted"));
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
      if (buffer.length > 2_200_000) {
        finish(reject, new Error("Tool result read response exceeded its limit"));
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
      if (!settled) finish(reject, new Error("Tool result read connection closed"));
    });
  });
}

export default function registerAgentHubToolResultRead(pi) {
  pi.registerTool({
    name: "agent_hub_integration_tool_result_read",
    label: "Read archived integration tool result",
    description:
      "Reads a large integration tool result that was truncated in context. " +
      'mode="size" returns metadata (size_bytes, artifact_id, artifact_reason). ' +
      'mode="range" returns a text slice with offset/limit/next_offset for paging. ' +
      "Use the tool_call_id from the truncated result summary.",
    parameters: {
      type: "object",
      properties: {
        tool_call_id: { type: "string", description: "Tool call id from the truncated result summary" },
        mode: { type: "string", enum: ["size", "range"], description: "size: metadata only; range: text slice" },
        offset: { type: "integer", description: "Byte offset for range reads (default 0)" },
        limit: { type: "integer", description: "Max bytes for range reads (default 65536)" },
      },
      required: ["tool_call_id", "mode"],
    },
    async execute(args, { signal }) {
      const response = await callBroker(args, signal);
      if (response.error) throw new Error(response.error);
      return {
        content: [{ type: "text", text: JSON.stringify(response, null, 2) }],
      };
    },
  });
}
"#;

const PI_VISION_EXTENSION_SOURCE: &str = r#"
import { createConnection } from "node:net";

const portValue = process.env.AGENT_HUB_VISION_PORT;
const token = process.env.AGENT_HUB_VISION_TOKEN;
if (!portValue || !token) {
  throw new Error("Agent Hub vision analysis broker is not configured");
}

function callBroker(params, signal) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let buffer = "";
    const socket = createConnection({ host: "127.0.0.1", port: Number(portValue) });
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      signal?.removeEventListener("abort", onAbort);
      socket.destroy();
      callback(value);
    };
    const onAbort = () => finish(reject, new Error("Vision analysis aborted"));
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
        finish(reject, new Error("Vision analysis response exceeded its limit"));
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
      if (!settled) finish(reject, new Error("Vision analysis broker disconnected"));
    });
  });
}

export default function registerAgentHubVisionAnalyze(pi) {
  pi.registerTool({
    name: "vision_analyze",
    label: "Vision Analyze",
    description: "Analyze an image in the workspace using the agent's vision model. Uploaded chat attachments are available under /workspace/attachments/<name>.",
    promptSnippet: "vision_analyze(image_path, prompt?): describe or analyze the image at image_path.",
    executionMode: "sequential",
    parameters: {
      type: "object",
      additionalProperties: false,
      required: ["image_path"],
      properties: {
        image_path: { type: "string", description: "Path to the image inside the workspace, for example /workspace/attachments/photo.png" },
        prompt: { type: "string", description: "Optional analysis prompt; defaults to describing the image" }
      }
    },
    async execute(_toolCallId, params, signal) {
      const response = await callBroker(params, signal);
      if (!response.ok) throw new Error(response.error || "Vision analysis failed");
      return {
        content: [{ type: "text", text: response.text || "" }],
        details: response,
      };
    },
  });
}

"#;

pub(super) fn pi_agent_directory(run_env: &RunEnv) -> PathBuf {
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

fn pi_vision_extension_path(run_env: &RunEnv) -> PathBuf {
    pi_agent_directory(run_env).join(PI_VISION_EXTENSION_FILE)
}

fn pi_tool_result_extension_path(run_env: &RunEnv) -> PathBuf {
    pi_agent_directory(run_env).join("agent-hub-tool-result.mjs")
}

fn materialize_tool_result_extension(run_env: &RunEnv, enabled: bool) -> anyhow::Result<()> {
    let extension_path = pi_tool_result_extension_path(run_env);
    if !enabled {
        return remove_file_if_present(&extension_path);
    }
    prepare_control_directory(&pi_agent_directory(run_env), "Pi Agent directory")?;
    replace_private_file(&extension_path, PI_TOOL_RESULT_EXTENSION_SOURCE.as_bytes())
        .context("write tool result read Extension")
}

fn materialize_skill_exec_extension(run_env: &RunEnv, enabled: bool) -> anyhow::Result<()> {
    let extension_path = pi_skill_exec_extension_path(run_env);
    if !enabled {
        return remove_file_if_present(&extension_path);
    }
    prepare_control_directory(&pi_agent_directory(run_env), "Pi Agent directory")?;
    replace_private_file(&extension_path, SKILL_EXEC_EXTENSION_SOURCE.as_bytes())
        .context("write Skill execution Extension")
}

fn materialize_vision_extension(run_env: &RunEnv, enabled: bool) -> anyhow::Result<()> {
    let extension_path = pi_vision_extension_path(run_env);
    if !enabled {
        return remove_file_if_present(&extension_path);
    }
    prepare_control_directory(&pi_agent_directory(run_env), "Pi Agent directory")?;
    replace_private_file(&extension_path, PI_VISION_EXTENSION_SOURCE.as_bytes())
        .context("write vision analysis Extension")
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
            // These files are control-created read-only Pi sources (integration
            // tools, Skill execution extension). They must never be
            // owner-writable by the sandbox user: a live session process could
            // otherwise open a write descriptor during the next Run preparation
            // and keep tampering after the final protect step.
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt;
                use std::os::unix::fs::PermissionsExt;
                let cpath =
                    CString::new(path.as_os_str().as_bytes()).map_err(std::io::Error::other)?;
                // SAFETY: pointer is NUL-terminated and the path is valid.
                if unsafe { libc::chown(cpath.as_ptr(), 0, PI_SANDBOX_GID) } != 0 {
                    return Err(std::io::Error::last_os_error())
                        .context("chown private file to root:agenthub");
                }
                fs::set_permissions(path, fs::Permissions::from_mode(0o440))
                    .context("protect private file as read-only")?;
            }
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

    prepare_control_directory(&pi_agent_directory(run_env), "Pi Agent directory")?;
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
        chown_private_path_if_root(path).with_context(|| format!("chown {purpose}"))?;
    }
    Ok(())
}

/// Creates a control-owned directory that the sandbox user can traverse but
/// never write: used for the Pi agent directory and the Skill packages root
/// so a live sandbox process cannot chmod/rename protected sources inside
/// them between materialization steps.
pub(super) fn prepare_control_directory(path: &Path, purpose: &str) -> anyhow::Result<()> {
    fs::create_dir_all(path).with_context(|| format!("create {purpose}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::PermissionsExt;
        let cpath = CString::new(path.as_os_str().as_bytes()).map_err(std::io::Error::other)?;
        // SAFETY: pointer is NUL-terminated and the path is valid.
        if unsafe { libc::chown(cpath.as_ptr(), 0, PI_SANDBOX_GID) } != 0 {
            return Err(std::io::Error::last_os_error()).context("chown control directory");
        }
        fs::set_permissions(path, fs::Permissions::from_mode(0o550))
            .with_context(|| format!("protect {purpose}"))?;
    }
    Ok(())
}
#[cfg(unix)]
fn chown_private_path_if_root(path: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;

    // The Runtime control process runs as root to create per-Session mount
    // namespaces; files and directories it materializes must be owned by the
    // sandbox user so the dropped Pi/Skill children can use them. Tests and
    // unprivileged environments keep their own ownership.
    if unsafe { libc::geteuid() } != 0 {
        return Ok(());
    }
    let path = CString::new(path.as_os_str().as_bytes()).map_err(std::io::Error::other)?;
    // SAFETY: pointer is NUL-terminated and the path is valid.
    if unsafe { libc::chown(path.as_ptr(), PI_SANDBOX_UID, PI_SANDBOX_GID) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(unix))]
fn chown_private_path_if_root(_path: &Path) -> std::io::Result<()> {
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
pub(super) enum LandlockPathKind {
    Directory,
    File,
}

#[cfg(target_os = "linux")]
pub(super) struct SessionMounts {
    #[cfg_attr(test, allow(dead_code))]
    workspace: CString,
    #[cfg_attr(test, allow(dead_code))]
    engine_state: CString,
    #[cfg_attr(test, allow(dead_code))]
    tmp: CString,
    #[cfg_attr(test, allow(dead_code))]
    workspace_dst: CString,
    #[cfg_attr(test, allow(dead_code))]
    agent_state_dst: CString,
    #[cfg_attr(test, allow(dead_code))]
    tmp_dst: CString,
}

#[cfg(target_os = "linux")]
impl SessionMounts {
    #[cfg_attr(test, allow(dead_code))]
    pub(super) fn new(workspace: &Path, engine_state: &Path, tmp: &Path) -> std::io::Result<Self> {
        let path_to_c = |path: &Path| -> std::io::Result<CString> {
            CString::new(path.as_os_str().as_bytes()).map_err(std::io::Error::other)
        };
        Ok(Self {
            workspace: path_to_c(workspace)?,
            engine_state: path_to_c(engine_state)?,
            tmp: path_to_c(tmp)?,
            workspace_dst: CString::new(PI_WORKSPACE_MOUNT).map_err(std::io::Error::other)?,
            agent_state_dst: CString::new(PI_AGENT_STATE_MOUNT).map_err(std::io::Error::other)?,
            tmp_dst: CString::new(PI_TMP_MOUNT).map_err(std::io::Error::other)?,
        })
    }

    /// Installs the per-Session mount namespace inside the pre-exec hook.
    /// Every step is a raw syscall with precomputed CStrings; any failure
    /// aborts the child so an unisolated Session can never start.
    #[cfg_attr(test, allow(dead_code))]
    pub(super) fn apply(&self) -> std::io::Result<()> {
        // SAFETY: scalar arguments only.
        if unsafe { libc::unshare(libc::CLONE_NEWNS) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // Detach the inherited (possibly shared) mount tree before bind
        // mounts; source/type/data are NULL and the flag is scalar.
        // SAFETY: the target "/" is a static NUL-terminated literal.
        if unsafe {
            libc::mount(
                std::ptr::null(),
                c"/".as_ptr(),
                std::ptr::null(),
                libc::MS_REC | libc::MS_PRIVATE,
                std::ptr::null(),
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error());
        }
        for target in [&self.workspace_dst, &self.agent_state_dst, &self.tmp_dst] {
            // SAFETY: target is NUL-terminated; EEXIST is expected on reruns.
            if unsafe { libc::mkdir(target.as_ptr(), 0o755) } != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::EEXIST) {
                    return Err(error);
                }
            }
        }
        let bind = |source: &CString, destination: &CString| -> std::io::Result<()> {
            // SAFETY: both pointers are NUL-terminated and stay live;
            // MS_BIND requires no filesystem type.
            if unsafe {
                libc::mount(
                    source.as_ptr(),
                    destination.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND,
                    std::ptr::null(),
                )
            } != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        };
        bind(&self.workspace, &self.workspace_dst)?;
        bind(&self.engine_state, &self.agent_state_dst)?;
        bind(&self.tmp, &self.tmp_dst)?;
        // /agent-state must be read-only at the VFS level so truncate(2) and
        // other write syscalls that Landlock cannot fully cover are blocked.
        // SAFETY: remount only needs the NUL-terminated target.
        if unsafe {
            libc::mount(
                std::ptr::null(),
                self.agent_state_dst.as_ptr(),
                std::ptr::null(),
                libc::MS_REMOUNT | libc::MS_BIND | libc::MS_RDONLY,
                std::ptr::null(),
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg_attr(test, allow(dead_code))]
    pub(super) fn chdir_workspace(&self) -> std::io::Result<()> {
        // SAFETY: pointer is NUL-terminated.
        if unsafe { libc::chdir(self.workspace_dst.as_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
#[cfg_attr(test, allow(dead_code))]
pub(super) fn apply_mount_landlock_rules(
    ruleset_fd: i32,
    rules: &[(CString, LandlockPathKind, u64)],
) -> std::io::Result<()> {
    for (path, kind, access) in rules {
        add_landlock_path_rule_raw(ruleset_fd, path, *kind, *access)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
struct PiFilesystemSandbox {
    rules: Vec<(CString, LandlockPathKind, u64)>,
    #[cfg_attr(test, allow(dead_code))]
    mounts: SessionMounts,
    #[cfg_attr(test, allow(dead_code))]
    mount_rules: Vec<(CString, LandlockPathKind, u64)>,
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
            .any(|tool| matches!(tool.as_str(), "read" | "grep" | "find" | "ls"));
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

        let rules = rules
            .into_iter()
            .map(|((path, kind), access)| {
                let path = CString::new(path.as_os_str().as_bytes())
                    .context("Landlock path contains a NUL byte")?;
                Ok((path, kind, access))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mounts = SessionMounts::new(&run_env.workdir, &run_env.engine_state_root, pi_temp)
            .context("prepare Session mount paths")?;
        let mut mount_rules = Vec::new();
        mount_rules.push((
            mounts.workspace_dst.clone(),
            LandlockPathKind::Directory,
            workspace_access,
        ));
        mount_rules.push((
            mounts.agent_state_dst.clone(),
            LandlockPathKind::Directory,
            LANDLOCK_DIRECTORY_READ_ONLY,
        ));
        if secret_readable {
            mount_rules.push((
                CString::new(format!("{PI_AGENT_STATE_MOUNT}/secrets"))
                    .context("Secret mount path contains a NUL byte")?,
                LandlockPathKind::Directory,
                LANDLOCK_DIRECTORY_LIST | LANDLOCK_ACCESS_FS_READ_FILE,
            ));
        }
        mount_rules.push((
            mounts.tmp_dst.clone(),
            LandlockPathKind::Directory,
            LANDLOCK_RUNTIME_WRITE,
        ));
        Ok(Self {
            rules,
            mounts,
            mount_rules,
        })
    }

    fn apply_inside_pre_exec(&self) -> std::io::Result<()> {
        #[cfg(not(test))]
        {
            self.mounts.apply()?;
        }

        let ruleset_fd = create_landlock_ruleset_raw()?;
        let apply_rule =
            |path: &CString, kind: LandlockPathKind, access: u64| -> std::io::Result<()> {
                add_landlock_path_rule_raw(ruleset_fd, path, kind, access)
            };
        for (path, kind, access) in &self.rules {
            apply_rule(path, *kind, *access)?;
        }
        #[cfg(not(test))]
        {
            apply_mount_landlock_rules(ruleset_fd, &self.mount_rules)?;
        }
        // Installs the /proc/self/maps rule (after fork), no_new_privs, the
        // Landlock restriction, and closes the ruleset descriptor.
        restrict_landlock_child(ruleset_fd)?;
        #[cfg(not(test))]
        {
            // SAFETY: these syscalls only use scalar arguments.
            if unsafe { libc::setgid(PI_SANDBOX_GID) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if unsafe { libc::setuid(PI_SANDBOX_UID) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        #[cfg(not(test))]
        {
            // SAFETY: pointer is NUL-terminated.
            if unsafe { libc::chdir(self.mounts.workspace_dst.as_ptr()) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(())
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
fn create_landlock_ruleset_raw() -> std::io::Result<i32> {
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
        return Err(std::io::Error::last_os_error());
    }
    let raw_fd = i32::try_from(raw_fd).map_err(std::io::Error::other)?;
    // SAFETY: the descriptor is valid; FD_CLOEXEC keeps it out of exec'd images.
    if unsafe { libc::fcntl(raw_fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(raw_fd);
        }
        return Err(error);
    }
    Ok(raw_fd)
}

#[cfg(target_os = "linux")]
fn add_landlock_path_rule_raw(
    ruleset_fd: i32,
    path: &CString,
    kind: LandlockPathKind,
    access: u64,
) -> std::io::Result<()> {
    let mut flags = libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW;
    if kind == LandlockPathKind::Directory {
        flags |= libc::O_DIRECTORY;
    }
    // SAFETY: `path` is NUL-terminated and remains live for the syscall.
    let raw_fd = unsafe { libc::open(path.as_ptr(), flags) };
    if raw_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let rule = LandlockPathBeneathAttr {
        allowed_access: access,
        parent_fd: raw_fd,
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
    // SAFETY: the descriptor was opened above and is no longer needed.
    unsafe {
        libc::close(raw_fd);
    }
    if result < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
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
    vision_broker: Option<VisionAnalyzeBroker>,
    tool_result_broker: Option<ToolResultBroker>,
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
        vision: Option<VisionAnalyzeConfig>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(!tools.is_empty(), "Pi tool allowlist must not be empty");
        let session_dir = run_env.engine_state_root.join(PI_SESSION_DIRECTORY);
        let agent_dir = pi_agent_directory(run_env);
        let pi_home = pi_home_directory(run_env);
        let pi_temp = pi_temp_directory(run_env);
        for (path, purpose) in [
            (&session_dir, "Pi Session directory"),
            (&pi_home, "Pi Home directory"),
            (&pi_temp, "Pi temporary directory"),
        ] {
            prepare_private_directory(path, purpose)?;
        }
        prepare_control_directory(&agent_dir, "Pi Agent directory")?;
        let agent_cache = agent_dir.join("cache");
        prepare_private_directory(&agent_cache, "Pi Agent cache directory")?;
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
        let vision_enabled = tools.iter().any(|tool| tool == "vision_analyze");
        let vision_broker = match (vision_enabled, vision) {
            (true, Some(config)) => {
                materialize_vision_extension(run_env, true)?;
                Some(
                    VisionAnalyzeBroker::start(run_env, config)
                        .context("start vision analysis broker")?,
                )
            }
            (true, None) => {
                warn!("vision_analyze enabled without model proxy configuration; skipping");
                materialize_vision_extension(run_env, false)?;
                None
            }
            (false, _) => {
                materialize_vision_extension(run_env, false)?;
                None
            }
        };
        // 归档工具结果读取 broker：Hub 凭据已配置时启用（Pi 扩展经它调 Hub）。
        let tool_result_broker = (TOOL_RESULT_HUB_URL.get().is_some()
            && TOOL_RESULT_RUNTIME_TOKEN.get().is_some())
        .then(ToolResultBroker::start)
        .transpose()
        .context("start tool result broker")?;
        materialize_tool_result_extension(run_env, tool_result_broker.is_some())?;
        crate::protect_pi_agent_execution_sources(
            &agent_dir,
            &run_env.engine_state_root.join(crate::SKILL_EXEC_DIRECTORY),
            &run_env.engine_state_root.join("secrets"),
        )
        .context("protect Pi Agent execution sources")?;

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
        let vision_extension = pi_vision_extension_path(run_env);
        if vision_extension.is_file() {
            command.arg("--extension").arg(&vision_extension);
        }
        let tool_result_extension = pi_tool_result_extension_path(run_env);
        if tool_result_extension.is_file() {
            command.arg("--extension").arg(&tool_result_extension);
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
        apply_pi_secret_environment(&mut command, run_env);
        if let Some(broker) = &skill_exec_broker {
            command
                .env("AGENT_HUB_SKILL_EXEC_PORT", broker.port().to_string())
                .env("AGENT_HUB_SKILL_EXEC_TOKEN", broker.token());
        }
        if let Some(broker) = &vision_broker {
            command
                .env("AGENT_HUB_VISION_PORT", broker.port().to_string())
                .env("AGENT_HUB_VISION_TOKEN", broker.token());
        }
        if let Some(broker) = &tool_result_broker {
            command
                .env("AGENT_HUB_TOOL_RESULT_PORT", broker.port().to_string())
                .env("AGENT_HUB_TOOL_RESULT_TOKEN", broker.token());
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
            let sandbox = filesystem_sandbox;
            // The closure runs between fork and exec and performs only syscalls
            // plus syscall-level helpers prepared by the parent.
            unsafe {
                command.pre_exec(move || sandbox.apply_inside_pre_exec());
            }
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("spawn Pi RPC process: {pi_bin}"))?;
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
            vision_broker,
            tool_result_broker,
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
        if let Some(broker) = &self.vision_broker {
            broker.update_run(binding.id, binding.model_id.clone());
        }
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
        self.vision_broker.take();
    }
}

impl Drop for PersistentPiRpcProcess {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug, Clone)]
pub(super) struct VisionAnalyzeConfig {
    pub(super) model_proxy_base_url: String,
    pub(super) model_binding_id: uuid::Uuid,
    pub(super) model_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VisionAnalyzeRequest {
    token: String,
    image_path: String,
    #[serde(default)]
    prompt: Option<String>,
}

#[derive(Debug, Serialize)]
struct VisionAnalyzeResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    text: String,
}

struct VisionAnalyzeBrokerContext {
    workdir: PathBuf,
    token: String,
    stop: Arc<AtomicBool>,
    config: Arc<std::sync::Mutex<VisionAnalyzeConfig>>,
}

pub(super) struct VisionAnalyzeBroker {
    port: u16,
    token: String,
    stop: Arc<AtomicBool>,
    config: Arc<std::sync::Mutex<VisionAnalyzeConfig>>,
    actor: Option<thread::JoinHandle<()>>,
}

/// 读取归档工具结果的 loopback broker（Pi 扩展经它访问 Hub）。
/// 凭据在 runtime 启动时初始化（见 main.rs），broker 无需再传参。
pub(super) static TOOL_RESULT_HUB_URL: std::sync::OnceLock<String> = std::sync::OnceLock::new();
pub(super) static TOOL_RESULT_RUNTIME_TOKEN: std::sync::OnceLock<String> =
    std::sync::OnceLock::new();

pub(super) struct ToolResultBroker {
    port: u16,
    token: String,
    stop: Arc<AtomicBool>,
    actor: Option<thread::JoinHandle<()>>,
}

#[derive(serde::Deserialize)]
struct ToolResultBrokerRequest {
    token: String,
    tool_call_id: String,
    mode: String,
    #[serde(default)]
    offset: Option<i64>,
    #[serde(default)]
    limit: Option<i64>,
}

impl ToolResultBroker {
    fn start() -> anyhow::Result<Self> {
        let listener =
            TcpListener::bind("127.0.0.1:0").context("bind tool result loopback listener")?;
        let port = listener
            .local_addr()
            .context("read tool result listener address")?
            .port();
        listener
            .set_nonblocking(true)
            .context("configure tool result listener")?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let context = ToolResultBrokerContext {
            token: token.clone(),
            stop: Arc::clone(&stop),
        };
        let actor = thread::spawn(move || run_tool_result_broker(listener, &context));
        Ok(Self {
            port,
            token,
            stop,
            actor: Some(actor),
        })
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn token(&self) -> &str {
        &self.token
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(actor) = self.actor.take() {
            let _ = actor.join();
        }
    }
}

impl Drop for ToolResultBroker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct ToolResultBrokerContext {
    token: String,
    stop: Arc<AtomicBool>,
}

fn run_tool_result_broker(
    listener: TcpListener,
    context: &ToolResultBrokerContext,
) -> ! {
    loop {
        if context.stop.load(Ordering::Acquire) {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let context = ToolResultBrokerContext {
                    token: context.token.clone(),
                    stop: Arc::clone(&context.stop),
                };
                thread::spawn(move || {
                    let _ = handle_tool_result_connection(stream, &context);
                });
            }
            Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(_) => break,
        }
    }
    unreachable!("tool result broker loop exits only on shutdown");
}

fn handle_tool_result_connection(
    mut stream: std::net::TcpStream,
    context: &ToolResultBrokerContext,
) -> anyhow::Result<()> {
    use std::io::{BufRead, BufReader, Write};
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(());
    }
    let request: ToolResultBrokerRequest = serde_json::from_str(&line)
        .context("parse tool result broker request")?;
    if request.token != context.token {
        write_json_line(&mut stream, &serde_json::json!({ "error": "invalid broker token" }))?;
        return Ok(());
    }
    let response = fetch_tool_result(&request);
    write_json_line(&mut stream, &response)?;
    Ok(())
}

fn fetch_tool_result(request: &ToolResultBrokerRequest) -> serde_json::Value {
    let Some(hub_url) = TOOL_RESULT_HUB_URL.get() else {
        return serde_json::json!({ "error": "tool result broker is not configured" });
    };
    let Some(runtime_token) = TOOL_RESULT_RUNTIME_TOKEN.get() else {
        return serde_json::json!({ "error": "tool result broker is not configured" });
    };
    let mut url = format!(
        "{hub_url}/api/runtime/tool-results/{}?mode={}",
        request.tool_call_id, request.mode
    );
    if request.mode == "range" {
        url.push_str(&format!(
            "&offset={}&limit={}",
            request.offset.unwrap_or(0),
            request.limit.unwrap_or(64 * 1024)
        ));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let Ok(runtime) = runtime else {
        return serde_json::json!({ "error": "tool result broker runtime failed" });
    };
    runtime.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build();
        let Ok(client) = client else {
            return serde_json::json!({ "error": "tool result broker HTTP client failed" });
        };
        let response = client.get(url).bearer_auth(runtime_token).send().await;
        match response {
            Ok(response) => match response.error_for_status() {
                Ok(response) => match response.json::<serde_json::Value>().await {
                    Ok(value) => value,
                    Err(error) => {
                        serde_json::json!({ "error": format!("tool result read failed: {error}") })
                    }
                },
                Err(error) => {
                    serde_json::json!({ "error": format!("tool result read failed: {error}") })
                }
            },
            Err(error) => {
                serde_json::json!({ "error": format!("tool result read failed: {error}") })
            }
        }
    })
}

fn write_json_line(stream: &mut std::net::TcpStream, value: &serde_json::Value) -> anyhow::Result<()> {
    use std::io::Write;
    let mut encoded = serde_json::to_string(value)?;
    encoded.push('\n');
    stream.write_all(encoded.as_bytes())?;
    Ok(())
}

impl VisionAnalyzeBroker {
    fn start(run_env: &RunEnv, config: VisionAnalyzeConfig) -> anyhow::Result<Self> {
        let listener =
            TcpListener::bind("127.0.0.1:0").context("bind vision analysis loopback listener")?;
        let port = listener
            .local_addr()
            .context("read vision analysis listener address")?
            .port();
        listener
            .set_nonblocking(true)
            .context("configure vision analysis listener")?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let config = Arc::new(std::sync::Mutex::new(config));
        let context = VisionAnalyzeBrokerContext {
            workdir: run_env.workdir.clone(),
            token: token.clone(),
            stop: Arc::clone(&stop),
            config: Arc::clone(&config),
        };
        let actor = thread::spawn(move || run_vision_broker(listener, &context));
        Ok(Self {
            port,
            token,
            stop,
            config,
            actor: Some(actor),
        })
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn token(&self) -> &str {
        &self.token
    }

    fn update_run(&self, model_binding_id: uuid::Uuid, model_id: String) {
        if let Ok(mut config) = self.config.lock() {
            config.model_binding_id = model_binding_id;
            config.model_id = model_id;
        }
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(actor) = self.actor.take() {
            let _ = actor.join();
        }
    }
}

impl Drop for VisionAnalyzeBroker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_vision_broker(listener: TcpListener, context: &VisionAnalyzeBrokerContext) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            warn!(error = %error, "failed to build vision analysis runtime");
            return;
        }
    };
    while !context.stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => handle_vision_connection(stream, context, &runtime),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(_) => break,
        }
    }
}

fn handle_vision_connection(
    mut stream: TcpStream,
    context: &VisionAnalyzeBrokerContext,
    runtime: &tokio::runtime::Runtime,
) {
    let response = (|| -> anyhow::Result<VisionAnalyzeResponse> {
        let request = read_vision_request(&mut stream, &context.stop)?;
        anyhow::ensure!(
            constant_time_eq(request.token.as_bytes(), context.token.as_bytes()),
            "invalid vision analysis token"
        );
        let image_path = resolve_vision_image_path(&context.workdir, &request.image_path)?;
        let metadata = fs::symlink_metadata(&image_path)
            .with_context(|| format!("inspect image {}", image_path.display()))?;
        anyhow::ensure!(metadata.is_file(), "image path is not a regular file");
        anyhow::ensure!(
            metadata.len() <= MAX_VISION_IMAGE_BYTES,
            "图片过大，无法读取"
        );
        let bytes = fs::read(&image_path)
            .with_context(|| format!("read image {}", image_path.display()))?;
        let mime = vision_image_mime(&image_path);
        let data_url = format!("data:{mime};base64,{}", base64_encode(&bytes));
        let prompt = request
            .prompt
            .as_deref()
            .filter(|prompt| !prompt.trim().is_empty())
            .unwrap_or(DEFAULT_VISION_PROMPT);
        let config = context
            .config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let body = build_vision_request_body(&config.model_id, prompt, &data_url);
        let text = runtime.block_on(post_vision_request(&config, &body))?;
        Ok(VisionAnalyzeResponse {
            ok: true,
            error: None,
            text,
        })
    })()
    .unwrap_or_else(|error| {
        let message = error.to_string();
        let message = if message == "图片过大，无法读取" {
            message
        } else {
            format!("视觉分析失败：{message}")
        };
        VisionAnalyzeResponse {
            ok: false,
            error: Some(message),
            text: String::new(),
        }
    });
    if let Ok(encoded) = serde_json::to_vec(&response) {
        let _ = stream.write_all(&encoded);
        let _ = stream.write_all(b"\n");
        let _ = stream.flush();
    }
    let _ = stream.shutdown(Shutdown::Both);
}

fn read_vision_request(
    stream: &mut TcpStream,
    stop: &AtomicBool,
) -> anyhow::Result<VisionAnalyzeRequest> {
    stream
        .set_read_timeout(Some(VISION_REQUEST_READ_TIMEOUT))
        .context("configure vision analysis request timeout")?;
    let started = Instant::now();
    let mut request = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        anyhow::ensure!(
            !stop.load(Ordering::Acquire),
            "vision analysis broker stopped"
        );
        anyhow::ensure!(
            started.elapsed() <= VISION_REQUEST_READ_TIMEOUT,
            "vision analysis request timed out"
        );
        match stream.read(&mut chunk) {
            Ok(0) => anyhow::bail!("vision analysis request disconnected"),
            Ok(read) => {
                let bytes = &chunk[..read];
                if let Some(newline) = bytes.iter().position(|byte| *byte == b"\n"[0]) {
                    request.extend_from_slice(&bytes[..newline]);
                    anyhow::ensure!(
                        bytes[newline + 1..].iter().all(u8::is_ascii_whitespace),
                        "vision analysis request contains trailing data"
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
            Err(error) => return Err(error).context("read vision analysis request"),
        }
        anyhow::ensure!(
            request.len() <= MAX_VISION_REQUEST_BYTES,
            "vision analysis request exceeded its limit"
        );
    }
    anyhow::ensure!(
        request.len() <= MAX_VISION_REQUEST_BYTES,
        "vision analysis request exceeded its limit"
    );
    serde_json::from_slice(&request).context("parse vision analysis request")
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

fn resolve_vision_image_path(workdir: &Path, image_path: &str) -> anyhow::Result<PathBuf> {
    anyhow::ensure!(
        !image_path.trim().is_empty() && image_path.len() <= 4096,
        "image path is invalid"
    );
    anyhow::ensure!(!image_path.as_bytes().contains(&0), "image path is invalid");
    let relative = if let Some(rest) = image_path.strip_prefix("/workspace") {
        rest.strip_prefix('/').unwrap_or("")
    } else if image_path.starts_with('/') {
        anyhow::bail!("image path must be inside the workspace");
    } else {
        image_path
    };
    let canonical_workdir = fs::canonicalize(workdir).context("resolve workspace directory")?;
    let resolved = workdir.join(relative);
    let canonical = fs::canonicalize(&resolved).context("resolve image path")?;
    anyhow::ensure!(
        canonical.starts_with(&canonical_workdir),
        "image path escapes the workspace"
    );
    Ok(canonical)
}

fn vision_image_mime(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("bmp") => "image/bmp",
        Some("tiff") | Some("tif") => "image/tiff",
        Some("svg") => "image/svg+xml",
        _ => "image/png",
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0] as u32;
        let second = chunk.get(1).copied().unwrap_or(0) as u32;
        let third = chunk.get(2).copied().unwrap_or(0) as u32;
        let combined = (first << 16) | (second << 8) | third;
        encoded.push(ALPHABET[(combined >> 18) as usize & 63] as char);
        encoded.push(ALPHABET[(combined >> 12) as usize & 63] as char);
        encoded.push(if chunk.len() > 1 {
            ALPHABET[(combined >> 6) as usize & 63] as char
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            ALPHABET[combined as usize & 63] as char
        } else {
            '='
        });
    }
    encoded
}

fn build_vision_request_body(model_id: &str, prompt: &str, image_data_url: &str) -> Value {
    json!({
        "model": model_id,
        "input": [{
            "role": "user",
            "content": [
                { "type": "input_text", "text": prompt },
                { "type": "input_image", "image_url": image_data_url },
            ],
        }],
        "max_output_tokens": 1024,
    })
}

fn vision_response_text(value: &Value) -> anyhow::Result<String> {
    if let Some(outputs) = value.get("output").and_then(Value::as_array) {
        let mut chunks = Vec::new();
        for output in outputs {
            match output.get("type").and_then(Value::as_str) {
                Some("output_text") => {
                    if let Some(text) = output.get("text").and_then(Value::as_str) {
                        chunks.push(text);
                    }
                }
                Some("message") => {
                    if let Some(content) = output.get("content").and_then(Value::as_array) {
                        for part in content {
                            let text = match part.get("type").and_then(Value::as_str) {
                                Some("output_text" | "text") => {
                                    part.get("text").and_then(Value::as_str)
                                }
                                Some("refusal") => part.get("refusal").and_then(Value::as_str),
                                _ => None,
                            };
                            if let Some(text) = text.filter(|text| !text.trim().is_empty()) {
                                chunks.push(text);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        let text = chunks.join("");
        if !text.trim().is_empty() {
            return Ok(text);
        }
    }
    if let Some(text) = value
        .get("output_text")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
    {
        return Ok(text.to_owned());
    }
    anyhow::bail!("vision response contains no output text")
}

async fn post_vision_request(config: &VisionAnalyzeConfig, body: &Value) -> anyhow::Result<String> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(VISION_HTTP_TIMEOUT)
        .build()
        .context("build vision analysis HTTP client")?;
    let response = client
        .post(format!(
            "{}/responses",
            config.model_proxy_base_url.trim_end_matches('/')
        ))
        .bearer_auth(VISION_PROXY_API_KEY_PLACEHOLDER)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(
            "x-agent-hub-model-binding-id",
            config.model_binding_id.to_string(),
        )
        .header("x-agent-hub-vision", "1")
        .json(body)
        .send()
        .await
        .context("send vision analysis request through the local model proxy")?;
    let status = response.status();
    let text = response
        .text()
        .await
        .context("read vision analysis response")?;
    anyhow::ensure!(
        status.is_success(),
        "model proxy returned HTTP {status}: {text}"
    );
    let value: Value = serde_json::from_str(&text).context("parse vision analysis response")?;
    if value.get("output").and_then(Value::as_array).is_none()
        && value.get("output_text").and_then(Value::as_str).is_none()
    {
        warn!(status = %status, body = %text.chars().take(800).collect::<String>(),
            "vision analysis response has no output text");
    }
    vision_response_text(&value)
}

fn apply_pi_secret_environment(command: &mut Command, run_env: &RunEnv) {
    for secret in &run_env.secret_values {
        command.env(format!("AGENT_SECRET_{}", secret.name), &secret.value);
        let path = format!("/agent-state/secrets/{}", secret.name);
        command.env(format!("AGENT_SECRET_FILE_{}", secret.name), path);
    }
    for file in &run_env.secret_files {
        let path = format!("/agent-state/secrets/{}", file.name);
        command.env(format!("AGENT_SECRET_FILE_{}", file.name), path);
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
    thinking_ids: BTreeMap<u64, u64>,
    thinking_sequence: u64,
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
            thinking_ids: BTreeMap::new(),
            thinking_sequence: 0,
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
                self.thinking_ids.clear();
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
                // 总是发出 run 终态 status 事件（含正常 completed）：
                // 客户端用 content=final_status + payload.kind=pi_terminal 判定 run 真正结束。
                // 之前只在非 completed 时发，导致正常完成时客户端只能靠事件流静默猜终态。
                self.events.push(AppendRunEventRequest {
                    event_id: uuid::Uuid::new_v4(),
                    event_type: "status".into(),
                    role: None,
                    content: Some(self.final_status.clone()),
                    payload: json!({ "kind": "pi_terminal" }),
                    waiting_tool: None,
                });
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
                    event_id: uuid::Uuid::new_v4(),
                    event_type: "message_delta".into(),
                    role: Some("assistant".into()),
                    content: Some(delta.to_owned()),
                    payload: json!({ "stream": true, "source": "pi" }),
                    waiting_tool: None,
                });
            }
            "thinking_start" => {
                let content_index = pi_content_index(event)?;
                let index = match self.thinking_ids.get(&content_index) {
                    Some(&index) if !self.thinking_ended.contains(&index) => index,
                    _ => self.allocate_thinking_item_index(content_index),
                };
                if self.thinking_started.insert(index) {
                    self.events.push(pi_reasoning_event(index, "started", None));
                }
            }
            "thinking_delta" => {
                let content_index = pi_content_index(event)?;
                let index = self.thinking_item_index(content_index);
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
                let content_index = pi_content_index(event)?;
                let index = self.thinking_item_index(content_index);
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
                event_id: uuid::Uuid::new_v4(),
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
            event_id: uuid::Uuid::new_v4(),
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
            event_id: uuid::Uuid::new_v4(),
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
        if delta.is_empty() || previous.len() > MAX_TOOL_OUTPUT_EVENT_BYTES {
            // Once the accumulated tool output exceeds the event cap, stop
            // streaming deltas; the completed event carries the truncated tail.
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
            event_id: uuid::Uuid::new_v4(),
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
            event_id: uuid::Uuid::new_v4(),
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
            event_id: uuid::Uuid::new_v4(),
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
            event_id: uuid::Uuid::new_v4(),
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
            event_id: uuid::Uuid::new_v4(),
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
    if !tools.iter().any(|tool| tool == "vision_analyze") {
        tools.push("vision_analyze".into());
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
        let mut envelope = json!({
            "message": prompt,
            "attachments": context.attachments,
            "external_user": context.external_user
        });
        // 工具结果只发有效的那个：Client 多结果场景发 tool_results 数组；
        // 单结果（服务器集成）发 tool_result。避免模型同时看到同一份结果的
        // 单值与数组两种表示。
        if context.tool_results.is_empty() {
            if let Some(tool_result) = &context.tool_result {
                envelope["tool_result"] = tool_result.clone();
            }
        } else {
            envelope["tool_results"] = serde_json::to_value(&context.tool_results)?;
        }
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

impl PiRunState {
    fn thinking_item_index(&mut self, content_index: u64) -> u64 {
        *self.thinking_ids.entry(content_index).or_insert_with(|| {
            let index = self.thinking_sequence;
            self.thinking_sequence += 1;
            index
        })
    }

    fn allocate_thinking_item_index(&mut self, content_index: u64) -> u64 {
        let index = self.thinking_sequence;
        self.thinking_sequence += 1;
        self.thinking_ids.insert(content_index, index);
        index
    }
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
        event_id: uuid::Uuid::new_v4(),
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
        // Tool output may contain raw binary (for example `head` on an ELF
        // file); NUL bytes cannot be stored in PostgreSQL jsonb and previously
        // failed the whole Run event upload, so replace them before upload.
        // Keep only a bounded tail in the event so a huge `cat` cannot bloat
        // the database; the marker line and structured fields stay visible.
        let output = output.replace('\0', "\u{FFFD}");
        if output.len() > MAX_TOOL_OUTPUT_EVENT_BYTES {
            let original_len = output.len();
            let tail_start = output.len() - MAX_TOOL_OUTPUT_EVENT_BYTES;
            let tail_start = (tail_start..output.len())
                .find(|index| output.is_char_boundary(*index))
                .unwrap_or(output.len());
            payload["output"] = json!(format!(
                "[output truncated: {original_len} bytes]\n{}",
                &output[tail_start..]
            ));
            payload["output_truncated"] = json!(true);
            payload["output_size_bytes"] = json!(original_len);
        } else {
            payload["output"] = json!(output);
        }
    }
    if let Some(success) = success {
        payload["success"] = json!(success);
        payload["status"] = json!(if success { "completed" } else { "failed" });
    }
    AppendRunEventRequest {
        event_id: uuid::Uuid::new_v4(),
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
        event_id: uuid::Uuid::new_v4(),
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
        collections::BTreeMap,
        os::unix::process::CommandExt,
        process::{Command, Output, Stdio},
    };

    use super::*;
    use crate::RunEnv;
    use agent_hub_shared::{RunSecretFileDto, RunSecretValueDto};

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
            command.pre_exec(move || sandbox.apply_inside_pre_exec());
        }
        command.output().unwrap()
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

    #[test]
    fn secret_environment_is_injected_into_pi_process_command() {
        let temp = tempfile::tempdir().unwrap();
        let engine_state_root = temp.path().join("sessions/first/engine-state");
        let run_env = RunEnv {
            workdir: temp.path().join("sessions/first/workspace"),
            engine_state_root: engine_state_root.clone(),
            hub_url: "http://127.0.0.1:8080".into(),
            maintenance_token_file: None,
            secret_values: vec![
                RunSecretValueDto {
                    name: "TOKEN".into(),
                    value: "value-one".into(),
                },
                RunSecretValueDto {
                    name: "API_KEY".into(),
                    value: "value-two".into(),
                },
            ],
            secret_files: vec![
                RunSecretFileDto {
                    name: "CREDENTIALS".into(),
                    size_bytes: 1,
                    sha256: "checksum-one".into(),
                },
                RunSecretFileDto {
                    name: "CERT".into(),
                    size_bytes: 1,
                    sha256: "checksum-two".into(),
                },
            ],
        };
        let mut command = Command::new("pi");

        apply_pi_secret_environment(&mut command, &run_env);

        let envs = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            envs.get("AGENT_SECRET_TOKEN")
                .and_then(|value| value.as_deref()),
            Some("value-one")
        );
        assert_eq!(
            envs.get("AGENT_SECRET_API_KEY")
                .and_then(|value| value.as_deref()),
            Some("value-two")
        );
        assert_eq!(
            envs.get("AGENT_SECRET_FILE_TOKEN")
                .and_then(|value| value.as_deref()),
            Some("/agent-state/secrets/TOKEN")
        );
        assert_eq!(
            envs.get("AGENT_SECRET_FILE_API_KEY")
                .and_then(|value| value.as_deref()),
            Some("/agent-state/secrets/API_KEY")
        );
        assert_eq!(
            envs.get("AGENT_SECRET_FILE_CREDENTIALS")
                .and_then(|value| value.as_deref()),
            Some("/agent-state/secrets/CREDENTIALS")
        );
        assert_eq!(
            envs.get("AGENT_SECRET_FILE_CERT")
                .and_then(|value| value.as_deref()),
            Some("/agent-state/secrets/CERT")
        );
    }

    #[test]
    fn vision_request_body_contains_original_image_bytes() {
        let image_bytes = (0_u8..=255).cycle().take(4096).collect::<Vec<_>>();
        let data_url = format!("data:image/png;base64,{}", base64_encode(&image_bytes));
        let body = build_vision_request_body("vision-model", "describe", &data_url);

        assert_eq!(body["model"], "vision-model");
        assert_eq!(body["max_output_tokens"], 1024);
        let input = body["input"].as_array().unwrap();
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["role"], "user");
        let content = input[0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["type"], "input_text");
        assert_eq!(content[0]["text"], "describe");
        assert_eq!(content[1]["type"], "input_image");
        let url = content[1]["image_url"].as_str().unwrap();
        let encoded = url.strip_prefix("data:image/png;base64,").unwrap();
        assert_eq!(encoded, base64_encode(&image_bytes));
    }

    #[test]
    fn vision_image_mime_and_path_resolution_are_safe() {
        assert_eq!(vision_image_mime(Path::new("photo.PNG")), "image/png");
        assert_eq!(vision_image_mime(Path::new("photo.jpeg")), "image/jpeg");
        assert_eq!(vision_image_mime(Path::new("photo.unknown")), "image/png");

        let temp = tempfile::tempdir().unwrap();
        let workdir = temp.path().join("workspace");
        fs::create_dir_all(&workdir).unwrap();
        fs::write(workdir.join("photo.png"), b"image").unwrap();
        assert_eq!(
            resolve_vision_image_path(&workdir, "/workspace/photo.png").unwrap(),
            fs::canonicalize(workdir.join("photo.png")).unwrap()
        );
        assert!(resolve_vision_image_path(&workdir, "/etc/passwd").is_err());
        assert!(resolve_vision_image_path(&workdir, "../secret").is_err());
    }

    #[test]
    fn landlock_secret_files_are_readable_for_granted_read_capable_tools() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = isolated_run_env(temp.path(), "first");
        let secret_dir = fixture.run_env.engine_state_root.join("secrets");
        fs::create_dir_all(&secret_dir).unwrap();
        let secret_file = secret_dir.join("secret.txt");
        fs::write(&secret_file, "granted-secret\n").unwrap();

        let read_script = "IFS= read -r value < \"$1\"\n[ \"$value\" = \"granted-secret\" ]\nprintf 'secret-read-ok\\n'\n";
        for tools in [
            &["read", "grep", "find", "ls"][..],
            &["read", "bash"][..],
            &["read", "edit"][..],
            &["read", "write"][..],
        ] {
            let output = run_sandboxed_shell(&fixture, tools, read_script, &[&secret_file]);
            assert!(
                output.status.success(),
                "secrets must be readable with granted read-capable tools {:?}: {}",
                tools,
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                "secret-read-ok\n"
            );
        }

        let denied_by_bash_only = run_sandboxed_shell(
            &fixture,
            &["bash"],
            "if /bin/sh -c 'IFS= read -r _ < \"$1\"' sh \"$1\"; then exit 91; fi\nprintf 'secret-read-denied\\n'\n",
            &[&secret_file],
        );
        assert!(
            denied_by_bash_only.status.success(),
            "secrets must stay hidden from tools without read capability: {}",
            String::from_utf8_lossy(&denied_by_bash_only.stderr)
        );
        assert_eq!(
            String::from_utf8(denied_by_bash_only.stdout).unwrap(),
            "secret-read-denied\n"
        );
    }

    #[test]
    fn repeated_pi_content_indexes_get_distinct_reasoning_item_ids() {
        let mut state = PiRunState::new(
            uuid::Uuid::new_v4(),
            "native-session".into(),
            BTreeMap::new(),
        );
        for event in [
            json!({"type": "agent_start"}),
            json!({"type": "turn_start"}),
            json!({"type": "message_update", "assistantMessageEvent": {"type": "thinking_start", "contentIndex": 0}}),
            json!({"type": "message_update", "assistantMessageEvent": {"type": "thinking_delta", "contentIndex": 0, "delta": "one"}}),
            json!({"type": "message_update", "assistantMessageEvent": {"type": "thinking_end", "contentIndex": 0, "content": "one"}}),
            json!({"type": "message_update", "assistantMessageEvent": {"type": "thinking_start", "contentIndex": 0}}),
            json!({"type": "message_update", "assistantMessageEvent": {"type": "thinking_delta", "contentIndex": 0, "delta": "two"}}),
            json!({"type": "message_update", "assistantMessageEvent": {"type": "thinking_end", "contentIndex": 0, "content": "two"}}),
        ] {
            state.handle_event(&event).unwrap();
        }
        let mut item_ids = state
            .events
            .iter()
            .filter(|event| event.event_type == "item")
            .filter(|event| {
                event.payload.get("item_type").and_then(Value::as_str) == Some("reasoning")
            })
            .filter_map(|event| {
                event
                    .payload
                    .get("item_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        item_ids.sort();
        item_ids.dedup();
        assert_eq!(
            item_ids,
            vec!["pi-thinking-0".to_string(), "pi-thinking-1".to_string()]
        );
    }

    #[test]
    fn tool_output_sanitizes_nul_bytes_before_upload() {
        let args = json!({ "command": "head -c 8 /bin/ls" });
        let event = pi_tool_event(
            "tool-1",
            "bash",
            "completed",
            &args,
            Some("ELF\0binary\0"),
            Some(true),
        );
        assert_eq!(event.payload["output"], json!("ELF\u{FFFD}binary\u{FFFD}"));
        assert_eq!(event.payload["tool"], json!("bash"));
        assert_eq!(event.payload["success"], json!(true));

        let delta = pi_tool_event(
            "tool-2",
            "read",
            "output_delta",
            &json!({}),
            Some("text\0tail"),
            None,
        );
        assert_eq!(delta.payload["output"], json!("text\u{FFFD}tail"));
    }

    #[test]
    fn tool_output_truncates_oversized_output_with_marker() {
        let tail = "尾部🙂内容";
        let huge = format!("{}{}", "x".repeat(40 * 1024), tail);
        let event = pi_tool_event(
            "tool-3",
            "bash",
            "completed",
            &json!({ "command": "cat big.bin" }),
            Some(&huge),
            Some(true),
        );
        assert_eq!(event.payload["output_truncated"], json!(true));
        assert_eq!(event.payload["output_size_bytes"], json!(huge.len()));
        let rendered = event.payload["output"].as_str().unwrap();
        assert!(rendered.starts_with(&format!("[output truncated: {} bytes]\n", huge.len())));
        assert!(rendered.ends_with(tail));
        assert!(
            rendered.len() <= MAX_TOOL_OUTPUT_EVENT_BYTES + 64,
            "rendered output stayed bounded"
        );
    }

    #[test]
    fn tool_output_deltas_stop_after_the_cumulative_cap() {
        let mut state = PiRunState::new(
            uuid::Uuid::new_v4(),
            "native-session".into(),
            BTreeMap::new(),
        );
        let tool_call_id = "overflow-tool";
        for event in [
            json!({"type": "tool_execution_start", "toolCallId": tool_call_id, "toolName": "bash", "args": {"command": "yes"}}),
            json!({"type": "tool_execution_update", "toolCallId": tool_call_id, "toolName": "bash", "args": {"command": "yes"}, "partialResult": "x".repeat(30 * 1024)}),
            json!({"type": "tool_execution_update", "toolCallId": tool_call_id, "toolName": "bash", "args": {"command": "yes"}, "partialResult": "x".repeat(40 * 1024)}),
        ] {
            state.handle_event(&event).unwrap();
        }
        let deltas = state
            .events
            .iter()
            .filter(|event| {
                event.payload.get("phase").and_then(Value::as_str) == Some("output_delta")
            })
            .count();
        assert_eq!(deltas, 1, "only the delta before the cap is streamed");
    }
}
