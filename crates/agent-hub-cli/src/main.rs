use std::{
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use reqwest::{
    header::{AUTHORIZATION, CONTENT_TYPE},
    Method,
};
use serde_json::{json, Value};

#[derive(Parser)]
#[command(name = "agent-hub", version, about = "Manage an Agent Hub deployment")]
struct Cli {
    #[arg(long, env = "AGENT_HUB_HUB_URL")]
    hub_url: Option<String>,
    #[arg(long, env = "AGENT_HUB_API_KEY")]
    api_key: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Verify authentication and print the current user.
    Health,
    Agents(AgentsCommand),
    Sessions(SessionsCommand),
    Runtimes(RuntimesCommand),
    Models(ModelsCommand),
    Skills(SkillsCommand),
}

#[derive(Args)]
struct AgentsCommand {
    #[command(subcommand)]
    command: AgentsSubcommand,
}

#[derive(Subcommand)]
enum AgentsSubcommand {
    List,
    Get {
        id: String,
    },
    Update {
        id: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        instructions_file: Option<PathBuf>,
        #[arg(long)]
        visibility: Option<String>,
        #[arg(long)]
        add_skill: Vec<String>,
        #[arg(long)]
        remove_skill: Vec<String>,
    },
}

#[derive(Args)]
struct SessionsCommand {
    #[command(subcommand)]
    command: SessionsSubcommand,
}

#[derive(Subcommand)]
enum SessionsSubcommand {
    List,
}

#[derive(Args)]
struct RuntimesCommand {
    #[command(subcommand)]
    command: RuntimesSubcommand,
}

#[derive(Subcommand)]
enum RuntimesSubcommand {
    List,
}

#[derive(Args)]
struct ModelsCommand {
    #[command(subcommand)]
    command: ModelsSubcommand,
}

#[derive(Subcommand)]
enum ModelsSubcommand {
    List,
}

#[derive(Args)]
struct SkillsCommand {
    #[command(subcommand)]
    command: SkillsSubcommand,
}

#[derive(Subcommand)]
enum SkillsSubcommand {
    List,
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        description: String,
        #[arg(long)]
        content_file: PathBuf,
    },
    Delete {
        id: String,
    },
    Package {
        #[command(subcommand)]
        command: PackageSubcommand,
    },
}

#[derive(Subcommand)]
enum PackageSubcommand {
    Upload {
        id: String,
        #[arg(long)]
        dir: PathBuf,
    },
}

struct HubClient {
    base_url: String,
    api_key: String,
    http: reqwest::Client,
}

impl HubClient {
    fn new(base_url: String, api_key: String) -> Result<Self> {
        let base_url = base_url.trim_end_matches('/').to_owned();
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            bail!("AGENT_HUB_HUB_URL must be an absolute HTTP(S) URL");
        }
        if api_key.trim().is_empty() {
            bail!("missing Agent Hub API key");
        }
        Ok(Self {
            base_url,
            api_key,
            http: reqwest::Client::builder()
                .build()
                .context("build HTTP client")?,
        })
    }

    async fn json(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let mut request = self
            .http
            .request(method, &url)
            .header(AUTHORIZATION, format!("Bearer {}", self.api_key));
        if let Some(body) = body {
            request = request.header(CONTENT_TYPE, "application/json").json(&body);
        }
        let response = request.send().await.context("send Hub request")?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if status.is_success() {
            if text.trim().is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_str(&text).context("parse Hub JSON response");
        }
        let detail = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| truncate(&text, 500));
        bail!("Hub request {} failed ({}): {}", path, status, detail);
    }

    async fn upload_package(&self, skill_id: &str, dir: &Path) -> Result<Value> {
        let files = collect_package_files(dir)?;
        let boundary = format!("----agent-hub-cli-{}", std::process::id());
        let mut body = Vec::new();
        let manifest = serde_json::to_string(&json!({
            "paths": files.iter().map(|entry| entry.relative.clone()).collect::<Vec<_>>()
        }))
        .context("serialize package manifest")?;
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"manifest\"\r\n\r\n{manifest}\r\n"
            )
            .as_bytes(),
        );
        for (index, entry) in files.iter().enumerate() {
            let bytes = fs::read(&entry.path)
                .with_context(|| format!("read package file {}", entry.path.display()))?;
            let file_name = entry
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("file")
                .to_owned();
            body.extend_from_slice(
                format!(
                    "--{boundary}\r\nContent-Disposition: form-data; name=\"file-{index}\"; filename=\"{file_name}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
                )
                .as_bytes(),
            );
            body.extend_from_slice(&bytes);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        let url = format!("{}/api/skills/{skill_id}/package", self.base_url);
        let response = self
            .http
            .put(url)
            .header(AUTHORIZATION, format!("Bearer {}", self.api_key))
            .header(
                CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(body)
            .send()
            .await
            .context("upload Skill package")?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if status.is_success() {
            return serde_json::from_str(&text).context("parse Skill package response");
        }
        let detail = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| truncate(&text, 500));
        bail!("Skill package upload failed ({}): {}", status, detail);
    }
}

#[derive(Clone)]
struct PackageFile {
    path: PathBuf,
    relative: String,
}

fn collect_package_files(dir: &Path) -> Result<Vec<PackageFile>> {
    fn walk(dir: &Path, root: &Path, files: &mut Vec<PackageFile>) -> Result<()> {
        let mut entries = fs::read_dir(dir)
            .with_context(|| format!("read package directory {}", dir.display()))?
            .collect::<std::io::Result<Vec<_>>>()
            .context("read package directory entries")?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, root, files)?;
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .map_err(|_| anyhow!("package path escaped its directory"))?
                .to_string_lossy()
                .to_string();
            if relative.is_empty() || relative == ".." || relative.starts_with("../") {
                bail!("package path is unsafe: {relative}");
            }
            files.push(PackageFile { path, relative });
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(dir, dir, &mut files)?;
    if !files.iter().any(|file| file.relative == "SKILL.md") {
        bail!("Skill package must contain a root SKILL.md");
    }
    let mut seen = std::collections::BTreeSet::new();
    for file in &files {
        if !seen.insert(file.relative.clone()) {
            bail!("Skill package contains duplicate path {}", file.relative);
        }
    }
    Ok(files)
}

fn truncate(value: &str, limit: usize) -> String {
    let mut chars = value.chars();
    let head: String = chars.by_ref().take(limit).collect();
    if chars.next().is_some() {
        format!("{head}...")
    } else {
        head
    }
}

fn api_key_from_env(cli: &Cli) -> Result<String> {
    if let Some(key) = cli.api_key.clone().filter(|value| !value.trim().is_empty()) {
        return Ok(key);
    }
    if let Ok(file) = env::var("AGENT_HUB_API_KEY_FILE") {
        if !file.trim().is_empty() {
            let key = fs::read_to_string(&file)
                .context("read AGENT_HUB_API_KEY_FILE")?
                .trim()
                .to_owned();
            return Ok(key);
        }
    }
    bail!("missing Agent Hub API key: set AGENT_HUB_API_KEY, AGENT_HUB_API_KEY_FILE, or --api-key");
}

async fn run(cli: Cli) -> Result<()> {
    let hub_url = cli
        .hub_url
        .clone()
        .or_else(|| env::var("AGENT_HUB_HUB_URL").ok())
        .ok_or_else(|| anyhow!("missing Hub URL: set AGENT_HUB_HUB_URL or --hub-url"))?;
    let api_key = api_key_from_env(&cli)?;
    let client = HubClient::new(hub_url, api_key)?;

    match cli.command {
        Command::Health => {
            let me = client
                .json(Method::GET, "/api/auth/me", None)
                .await
                .context("Hub authentication check failed")?;
            println!("{}", pretty(&me));
        }
        Command::Agents(command) => match command.command {
            AgentsSubcommand::List => {
                let agents = client.json(Method::GET, "/api/agents", None).await?;
                println!("{}", pretty(&agents));
            }
            AgentsSubcommand::Get { id } => {
                let agent = client
                    .json(Method::GET, &format!("/api/agents/{id}"), None)
                    .await?;
                println!("{}", pretty(&agent));
            }
            AgentsSubcommand::Update {
                id,
                name,
                instructions_file,
                visibility,
                add_skill,
                remove_skill,
            } => {
                let current = client
                    .json(Method::GET, &format!("/api/agents/{id}"), None)
                    .await?;
                let mut skills = current
                    .get("managed_skill_ids")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for skill in remove_skill {
                    skills.retain(|value| value.as_str() != Some(skill.as_str()));
                }
                for skill in add_skill {
                    if !skills
                        .iter()
                        .any(|value| value.as_str() == Some(skill.as_str()))
                    {
                        skills.push(json!(skill));
                    }
                }
                let mut body = json!({
                    "name": current.get("name").cloned().unwrap_or(Value::Null),
                    "instructions": current.get("instructions").cloned().unwrap_or(Value::Null),
                    "visibility": current.get("visibility").cloned().unwrap_or(Value::Null),
                    "public_to": current.get("public_to").cloned().unwrap_or(Value::Null),
                    "runtime_id": current.get("runtime_id").cloned().unwrap_or(Value::Null),
                    "model_selection": current.get("model_selection").cloned().unwrap_or(Value::Null),
                    "model_settings": current.get("model_settings").cloned().unwrap_or(Value::Null),
                    "subagents": current.get("subagents").cloned().unwrap_or(Value::Null),
                    "sandbox_policy": current.get("sandbox_policy").cloned().unwrap_or(Value::Null),
                    "managed_skill_ids": skills,
                    "secret_declarations": current.get("secret_declarations").cloned().unwrap_or(Value::Null),
                    "mcp_allowlist": current.get("mcp_allowlist").cloned().unwrap_or(Value::Null),
                    "tool_allowlist": current.get("tool_allowlist").cloned().unwrap_or(Value::Null)
                });
                if let Some(name) = name {
                    body["name"] = json!(name);
                }
                if let Some(file) = instructions_file {
                    let instructions = fs::read_to_string(&file)
                        .with_context(|| format!("read instructions file {}", file.display()))?;
                    body["instructions"] = json!(instructions);
                }
                if let Some(visibility) = visibility {
                    body["visibility"] = json!(visibility);
                }
                let updated = client
                    .json(Method::PATCH, &format!("/api/agents/{id}"), Some(body))
                    .await?;
                println!("{}", pretty(&updated));
            }
        },
        Command::Sessions(command) => match command.command {
            SessionsSubcommand::List => {
                let sessions = client.json(Method::GET, "/api/sessions", None).await?;
                println!("{}", pretty(&sessions));
            }
        },
        Command::Runtimes(command) => match command.command {
            RuntimesSubcommand::List => {
                let runtimes = client.json(Method::GET, "/api/runtimes", None).await?;
                println!("{}", pretty(&runtimes));
            }
        },
        Command::Models(command) => match command.command {
            ModelsSubcommand::List => {
                let models = client
                    .json(Method::GET, "/api/model-connections", None)
                    .await?;
                println!("{}", pretty(&models));
            }
        },
        Command::Skills(command) => match command.command {
            SkillsSubcommand::List => {
                let skills = client.json(Method::GET, "/api/skills", None).await?;
                println!("{}", pretty(&skills));
            }
            SkillsSubcommand::Create {
                name,
                description,
                content_file,
            } => {
                let content = fs::read_to_string(&content_file)
                    .with_context(|| format!("read SKILL.md {}", content_file.display()))?;
                let skill = client
                    .json(
                        Method::POST,
                        "/api/skills",
                        Some(json!({
                            "name": name,
                            "description": description,
                            "content": content
                        })),
                    )
                    .await?;
                println!("{}", pretty(&skill));
            }
            SkillsSubcommand::Delete { id } => {
                let status = client
                    .json(Method::DELETE, &format!("/api/skills/{id}"), None)
                    .await?;
                println!("{}", pretty(&status));
            }
            SkillsSubcommand::Package { command } => match command {
                PackageSubcommand::Upload { id, dir } => {
                    let skill = client.upload_package(&id, &dir).await?;
                    println!("{}", pretty(&skill));
                }
            },
        },
    }
    Ok(())
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("agent-hub: {error:#}");
            ExitCode::FAILURE
        }
    }
}
