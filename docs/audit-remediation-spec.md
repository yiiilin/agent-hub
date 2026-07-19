# Plan 全面复核与修复 Spec

## 目标

本轮以仓库根目录 `plan.md` 为验收基线，补齐首轮实现中发现的安全、权限、
runtime 生命周期、Widget/Integration 协议和测试缺口。修复保持现有 Rust/Axum、
React/Vite、Postgres 和 Docker Compose 架构，前端构建产物由 backend 镜像直接托管，不引入与 V1 无关的产品能力。

## 功能批次与验收标准

### 1. Auth 与凭据边界

- password 使用 Argon2id；旧明文和 salted SHA-256 仅用于一次性登录迁移。
- browser session、runtime、embed session 和 automation webhook token 只以 SHA-256
  摘要落库，明文只在签发时返回。
- Runtime 仅在没有持久化凭据文件时需要一次性 `RUNTIME_ENROLLMENT_TOKEN`；backend
  不接受共享的长期注册 secret。
- password、session、API Key、Embed JWT 通过统一 `AuthProvider` / `SessionIssuer`
  边界接入；Integration token 仍不能进入用户控制面。
- OIDC mock callback 使用受控的 `email` 和 `sub` 参数创建或复用 member 用户。
- Widget token 只放 URL fragment 或授权 header，不进入 path、query 和 HTTP access log。

### 2. Agent 权限与 Runtime/Codex

- Agent 支持 `private`、`public_to` 和 `public`；`public_to` 使用明确的用户 UUID 列表。
- owner 可管理和调用；admin 可查看/治理任意 Agent，但不能调用未向其开放的 private
  Agent；member 只能查看和调用 public 或明确共享给自己的 Agent。
- 非 owner 响应不暴露 MCP、skills、model、sandbox、runtime 等控制面配置，UI 只展示
  服务端返回的 `can_manage` / `can_invoke` 所允许的操作。
- Agent 归档与 run 创建在同一数据库锁边界内；归档会终止 pending/running run，避免
  永久 pending 和归档后并发创建。
- runtime 定期回收 stale 节点和超 TTL workdir；Codex 支持 path/download 两种定位策略、
  thread resume 和 runtime local skills 合并，Hub/Agent 同名 skill 优先。
- app-server 子进程使用环境白名单和独立进程组；stderr/JSON-RPC error 不进入 Hub event；
  HTTP 请求有超时；文本 delta 会立即形成流式事件。

### 3. Skills、MCP、Sandbox 与 Model Proxy

- inline skill 的 name/content 都必须非空；SKILL.md frontmatter 和 Codex config 使用
  结构化序列化，换行和控制字符不能破坏 YAML/TOML。
- MCP secret 只写 `0600` per-run config；任何 app-server 错误都不能回显 config secret。
- runtime 上报 effective sandbox、降级原因、local skills 和 model proxy capability；
  管理台明确显示降级风险，且不存在 Runtime direct-model fallback。
- Hub 和 runtime model proxy client 均设置连接与总请求超时。

### 4. Automation、Widget 与 Integration

- scheduler 在事务内锁定并重新判断 due，同一 automation 在多 backend 实例下只触发一次。
- Automation 表单仅对 interval/cron 显示 schedule，切换类型会清空无效值；disabled trigger
  不显示可执行操作；webhook secret 仅创建时展示并通过 header 使用。
- Widget 只接受 `window.parent`、已绑定 origin 和 channel nonce 的消息；切换 session/run
  清空旧 transcript；`ready/resize/session-select/message-submit` 都有浏览器覆盖，resize
  携带 width/height。
- OAuth redirect 使用 URL API 编码 state；tool result 拒绝过期 request；非 UUID tool id
  的稳定 UUID 包含 run id，避免跨 run 冲突。

### 5. 部署与可维护性

- Hub backend、runtime 和 fake provider 容器以非 root 用户运行。
- `cargo fmt --check`、严格 Clippy、workspace tests、frontend build、npm audit 和完整
  Playwright 全部通过。
- 浏览器逐条验收 Auth、Agent/权限、Skills、MCP、Runtime/Codex、Model Proxy、Automation、
  Widget、Integration；每个功能域分别由质量 reviewer 和功能 reviewer 复审。

## 测试计划

- Rust 单元测试：Argon2/legacy 迁移、token 摘要、visibility 权限矩阵、inline skill/MCP
  validation、TOML/YAML 特殊字符、local skill 优先级、thread resume、delta streaming、
  tool request UUID、workdir GC、Codex 下载摘要和错误脱敏。
- Postgres/API 负路径：归档/run 并发边界、runtime token、scheduler 去重、expired tool result、
  OAuth state、public/private/public_to 与 admin/member 权限。
- Playwright：现有四条 spec 保留，并补 controlled OIDC、权限矩阵、Widget 完整协议、
  schedule 状态切换/cron/disabled/invalid webhook、token 不在 URL、integration 过期边界。
- 浏览器人工验收：在 Compose 环境逐功能执行真实 UI 操作，记录 console/network error 和
  desktop/mobile 截图；每个完整功能链至少执行一次。

## 非目标

- 不增加新的模型供应商 UI、复杂 cron 表达式、对象存储或外部 secret manager。
- 不把 runtime 变成用户级隔离边界；runtime 仍是管理员接入的可信执行基础设施。
- 不提交 `plan.md` 或本轮实施计划，不创建 Git commit。

## 二轮审查补充验收

### Auth 与启动配置

- OIDC mock 身份必须在 start 时写入一次性 state，callback 不接受覆盖身份；mock
  登录不能复用 admin 账号，重复或并发 callback 不能产生 500。
- production backend 不自动创建 Runtime enrollment；mock development 可显式提供
  `DEV_RUNTIME_ENROLLMENT_TOKEN` 预置一次 hash，消费后不得重放。注册 handler 拒绝
  空 Bearer，Runtime 已有受保护凭据文件时必须忽略 enrollment env。

### Agent、Widget 与 Automation 并发

- admin 保留治理权限，但与其他非 owner 一样看不到 Agent 的 model、sandbox、runtime、
  skills 和 MCP 控制面配置；切换 Agent 时不能短暂保留上一 Agent 的 selected run。
- Widget run stream 必须同时绑定 embed session id，不能仅凭同 Agent/owner 的另一个
  session 读取；切换 session 的旧请求不能回写新 transcript，失败或仍运行的旧 run
  不得永久阻塞下一条消息。
- Agent 归档、Automation 创建和 scheduler 统一数据库锁顺序；归档与 scheduler/创建并发
  不死锁、不留下 enabled Automation，也不产生归档后的 run。
- interval 与 cron 互切必须清空 schedule；创建新 webhook 后只保留最新一次性 secret，
  刷新后不再展示任何明文；无 cookie 的 webhook header 正向链必须通过。

### Integration 一致性

- 归档 Agent 后，既有 OAuth code 不能换 token，新 authorize/token 也不能签发；
  Integration 鉴权同时校验 Agent 未归档。
- SSE 在连接存续期间周期性重新校验 token 的过期、吊销和 Agent 归档状态。
- tool request id 无论输入是否 UUID 都必须带 run 作用域；tool-result follow-up 精确关联
  request/run，不能从 parent run 的其他结果串取上下文。
- 同一 Integration session 的并发 message 必须串行化，不能创建 sibling runs。

### Runtime、代理与部署

- 需要 workspace-write 的 run 不能分配到 effective read-only runtime；模型请求必须经
  Runtime loopback 和 Hub proxy，真实 API Key 只存在于 Hub。
- Hub/runtime model proxy 保留上游流式响应语义；runtime 对永久注册失败采用有界退避并
  暴露可用于 Compose 判活的 healthcheck。
- Docker build context 排除 `.env`、`target`、`node_modules` 和测试产物。

## 二轮测试计划

- Rust/API：覆盖 OIDC state 绑定与并发、空注册 token、owner/admin 脱敏、Widget session
  隔离、归档/OAuth/Automation 并发、Integration token 重校验、run scoped tool id、
  follow-up 精确关联、同 session message 串行化、runtime sandbox capability 匹配。
- Playwright：覆盖跨 Agent selected-run 清理、跨 Widget session SSE 拒绝、失败后重发、
  interval/cron 双向切换、webhook secret 一次性展示和无 cookie header 调用。
- 部署：验证四个应用容器 UID、healthcheck、迁移版本、凭据摘要长度和 Docker build
  context；随后重跑全量浏览器功能链与 desktop/mobile 截图检查。
