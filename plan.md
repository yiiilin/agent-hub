# agent-hub 设计计划

> Status: historical. This greenfield plan is superseded by the accepted ADRs,
> the current feature specs under `docs/`, and the active checklist in
> `docs/plans/2026-07-17-console-workflows-and-integration-apps.md`.

## Summary

- 在 `/Users/yiln/project/agent-hub` 现有空 git 仓库中创建绿地新项目，不 fork Hermes Hub，但沿用其技术栈：Rust 1.88+/Axum/Tokio/sqlx/Postgres，React 19/Vite/TypeScript，Docker Compose 部署。
- 产品形态：Multica 风格的 Agent Hub。Hub 是控制面，runtime 节点执行 Codex；Agent 是一等队友对象，支持 profile、instructions、skills、MCP、runtime、调用权限、自动化和外部平台嵌入。
- Codex 集成：runtime 为每个 run 创建独立 workdir 和 `CODEX_HOME`，启动 `codex app-server --listen stdio://`，通过 JSON-RPC 驱动 `initialize`、`thread/start|resume`、`turn/start`。
- 默认模型出口走 Hub 代理：Codex 访问 runtime 本地 OpenAI-compatible proxy，runtime 转发到 Hub，Hub 持有真实模型 provider key；允许管理员开启直连备用。
- V1 是平台完整版本，不是 runtime demo。

## Key Architecture

- **Workspace layout**
  - `backend`：Axum API、Postgres schema、auth、agent/task/automation/widget/external integration 控制面。
  - `runtime`：独立 Rust/Tokio binary + Docker image，负责注册 Hub、领取 runs、下载/定位 Codex、准备 per-run 环境、启动 app-server、流式回传事件。
  - `frontend`：React/Vite 管理台和 iframe widget。
  - `deploy`：Postgres + backend + runtime 可选 profile 的 Docker Compose。

- **Auth**
  - 插件式认证架构：统一 `AuthProvider` / `AuthPrincipal` / `SessionIssuer`。
  - V1 内置：password、OIDC、API Key、Embed JWT。
  - OAuth App + Session API 是外部平台授权协议，不混入用户登录 provider。
  - Agent 私有信息边界按 **Agent Owner**：admin 可治理/审计，但不能直接调用 private agent 使用 owner 的 MCP/OAuth/连接器。

- **Runtime and Codex**
  - runtime 通过注册 token 接入 Hub，上报 hostname、labels、Codex version、capabilities、sandbox mode、direct-model 是否启用。
  - Codex 版本由 runtime 自行配置；Hub 记录版本并按 capability 决定是否接受任务。
  - runtime 启动后可下载 Codex，也可使用已安装路径；下载策略由 runtime config 控制。
  - 每个 run 独立目录：
    - `workdir/`
    - `codex-home/`
    - `codex-home/config.toml`
    - `codex-home/skills/`
  - V1 运行数据只保留 runtime 本地目录；Hub 存 run 元数据、状态、日志摘要和消息，不做对象存储归档。

- **Config, Skills, MCP, Sandbox**
  - Skills 来源：Hub 管理 skills + runtime 本地 Codex skills；冲突时 Hub/agent skill 优先。
  - 每次 run 重建 `CODEX_HOME/skills`，避免 stale skill 泄漏。
  - MCP 默认显式 allowlist：空配置就是无 MCP，不继承 runtime 全局 Codex MCP。
  - MCP secret 写入 per-run `config.toml`，文件权限 `0600`，不走 argv，不写日志。
  - 沙盒采用 Multica 兼容策略：优先 `workspace-write + network_access`；平台限制导致不可用时允许降级，并在 runtime capability 和 UI 中显示风险。
  - Codex 模型 provider 默认写成 runtime 本地 proxy，例如 `http://127.0.0.1:<port>/v1`；runtime 用 scoped run token 转发到 Hub 模型代理。

## Public Interfaces

- **Hub Web/API**
  - Auth：`/api/auth/*`，支持 password/OIDC/session。
  - Agents：创建、更新、归档、权限、runtime 绑定、skills、MCP、model policy。
  - Runtime：注册、heartbeat、capability 上报、run claim、event append、run complete/fail。
  - Automations：cron/interval/manual/webhook trigger，目标是启动指定 Agent run。
  - Widget：iframe URL + short-lived embed token，宿主通过 `postMessage` 做 ready、resize、session selection、message submit。
  - Integrations：延续 Hermes Hub 风格 OAuth App + Session API，支持 session、messages、SSE events、attachments、tool request/result。

- **Runtime Protocol**
  - Runtime 使用 Hub-issued runtime token 注册。
  - Run 领取后，Hub 下发 agent config、instructions、skills manifest、MCP allowlist、sandbox policy、model proxy config。
  - Runtime 启动 Codex app-server，用 stdio JSON-RPC 驱动会话，并把 Codex text/tool/status/usage/session_id 映射成 Hub run events。
  - Run 完成后 runtime 保留本地 workdir 到 GC TTL，用于 resume；Hub 保存 `session_id` 和 `work_dir_ref`。

- **Widget**
  - V1 只支持 iframe + postMessage。
  - 外部平台用 Embed JWT 或 OAuth App 换 scoped embed session。
  - Widget 不接触平台主 token；只拿 Hub scoped session。

## Test Plan

- **Backend tests**
  - Auth provider contract：password、OIDC mock、API key、Embed JWT principal 解析。
  - Agent permissions：private agent 只有 owner 可 invoke，admin 可 view/manage 但不可借用 owner 私有连接。
  - MCP policy：空配置不继承全局 MCP；managed MCP 写入 allowlist；secret 不进入 logs/argv。
  - Runtime protocol：register、heartbeat、claim、event stream、complete/fail、capability mismatch。
  - Automation：cron/manual/webhook 创建 run，权限按触发者和 agent owner 计算。
  - Integration API：OAuth code flow、session create/message/SSE、tool request/result。

- **Runtime tests**
  - fake `codex` app-server 脚本模拟 initialize/thread/turn，验证 JSON-RPC 生命周期。
  - per-run `CODEX_HOME` 隔离：skills 重建、MCP allowlist、sandbox block、model provider proxy config。
  - Hub model proxy forwarding：Codex 请求 runtime proxy，runtime 带 run token 转发 Hub。
  - direct fallback：管理员启用后允许直连 provider，否则拒绝。

- **Frontend tests**
  - Agent 创建/编辑、runtime 状态、MCP redaction、automation 配置。
  - Widget iframe auth、postMessage ready/resize/send、SSE 渲染。
  - 权限 UI：private/public_to、owner/admin/member 差异。

- **E2E**
  - Docker Compose 启动 Postgres/backend/runtime。
  - 创建用户、创建 Agent、runtime 注册、发送 widget 消息、完成 Codex fake run。
  - 外部平台 OAuth App 创建 session，注册 tool，Agent 发起 tool request，平台回传 result。

## Assumptions

- 使用 `/Users/yiln/project/agent-hub` 现有空 git 仓库，不删除 `.git`，不从 hermes-hub fork。
- V1 不做对象存储归档；runtime 本地目录丢失时只能保留 Hub 中的消息/状态/日志摘要。
- Hub 模型代理是默认安全路径；Codex 直连 provider 是管理员显式开启的高级选项。
- MCP 默认不继承 Codex 全局配置，这是为了私有信息隔离优先。
- `grill-me` 对齐结果已锁定以上决策；实现前应先落一份 spec，再按 spec 拆 TDD 任务计划。
