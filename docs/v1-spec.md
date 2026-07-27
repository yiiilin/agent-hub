# Agent Hub V1 骨架 Spec

## 范围

V1 保留以下可浏览器验证的产品链路；其执行和存储边界已由 ADR-0012 及 `docs/session-runtime-spec.md` 更新为 Session 级：

1. 用户登录并获取 Hub 登录会话。
2. 创建 Agent，保存 Markdown instructions、visibility、owner、managed Skills、MCP、可调用工具、Runtime 约束、Agent Model Selection/Settings 和 Subagent Definitions；sandbox policy 仍参与执行但不在管理台展示。
3. Runtime 通过管理员签发的一次性 Enrollment Token 建立身份，之后使用自己的可撤销 Runtime Credential heartbeat。
4. 用户选择 Agent 后进入当前浏览器保留的 Conversation Draft；首条消息原子创建 Hub Session、Message 和 Run，后续消息继续该 Session，且每条消息独立持久化。
5. Runtime 获得 Session 的排他 ownership generation，在该 Session 独立的 `workspace/` 和 `engine-state/` 中继续同一个 Native Session。
6. Hub 保存消息、Run 和 native Item 映射事件，并通过 SSE 推送给管理台和 widget。
7. 管理台以 Session 为登录后默认页和第一导航，使用对话主视图展示消息、SSE 回复、按时间线折叠的执行活动与历史状态；Hub 内部状态、用量和 delta 事件不直接展示。
8. Widget iframe 支持旧 embed token、Integration App 后端为可信外部用户签发并自动续期的 15 分钟 Widget Access Credential，也支持管理员将普通 Integration App 配置为免登录公开 Widget。认证 Widget 按 App、Agent、Platform、Tenant 和 Identity 隔离；公开 Widget 按 App 与匿名 visitor key 隔离且不提供历史发现。
9. Session 离线时由 Runtime 生成最小 `tar.zst` Bundle，经 Hub 流式写入对象存储；恢复也只经过 Hub。
10. Integration App 统一 OAuth、外部 Session API 和 Widget，可委托多个 Agent，并通过应用级或用户级 Application Token 使用显式 Agent scopes。
11. Automation、Skill、API Key、Runtime 和 Administration 均以列表或专用 Tab 为主体，新建/编辑表单只在点击操作按钮后打开。
12. Model API Connection 以独立一级菜单管理 Global/Personal provider 地址、密钥、单一 API Type 和多个 Allowed Model ID；Agent/子 Agent 管理具体 Model Selection 和调用参数。System Default 保存 Global connection/model pair；Runtime 只使用 Run binding 和 Hub 的 Responses 入口，Hub 经内部无状态 Model Gateway 访问 OpenAI Responses、Chat Completions 或 Anthropic Messages provider。

## 兼容边界

- 既有 console、widget、integration 和 automation 用户链路继续可用；迁移只把它们接到统一 Session 生命周期。
- Hub-native Session 不伪造外部来源；外部 Session 固定绑定创建它的 External Platform、Tenant 和 Identity。
- Run 仍是调度、重试和审计记录，但不拥有 Workspace、Execution Engine state、Native Session 或引擎进程。
- Runtime 池属于管理员接入的可信基础设施；用户执行数据仍以 owner、Agent 权限、Session 隔离、Agent/App tool allowlist、MCP allowlist 和 sandbox 共同约束。

## 验收标准

- `docker compose up` 使用根目录 `compose.yml` 启动生产 Hub 与内部 Model Gateway；可选的同机 Runtime 通过 `runtime` profile 启动。
- `docker compose -f compose.dev.yml up` 启动 PostgreSQL、包含前端静态资源的 Hub backend、Model Gateway、同时支持 Responses/Chat Completions/Messages 的 fake provider 和 runtime 的完整开发环境。
- Model API Connection 可选择 `openai_responses`、`openai_chat_completions` 或 `anthropic_messages`，并开放 1 到 256 个精确 Model ID；三种协议的逐模型连接测试和 Runtime Responses 调用均经过 Gateway，Runtime 不获得 provider endpoint/API Key。
- Agent Model Settings 配置 reasoning、summary、verbosity、context、compaction、summary capability、service tier、provider retry/idle 和匹配 API Type 的 request settings；Subagent 可逐字段继承或覆盖。Run binding 固定有效配置，历史 usage/error 保留调用时 connection/API Type/model/settings 快照。
- Responses 到 Responses 保持字节透明；Chat/Messages 转换只覆盖 binding 中非空的协议专属参数。Runtime/Hub 不接受 connection-ID-only 模型请求。
- V1 不提供旧 one-connection/one-model schema、API 字段或 Run 兼容；开发和测试部署从空数据库建立最终 schema。
- 用户能登录管理台、创建 Agent、进入空白 Conversation Draft，并从主对话输入框发送首条消息创建 Session 和启动 Turn，看到关联 Run 从 pending/running 进入终态。失败的首条消息、刷新和关闭浏览器保留 Draft；成功发送、显式丢弃或退出登录按约定清除 Draft。
- 登录和根路径默认进入 Session 页；侧栏依次使用平台、具体 Agent 和搜索筛选，平台默认为“本平台”并可选择“全部平台”或某个具名 External Platform。所选可调用 Agent 同时过滤列表并决定新建对话使用的 Draft，不提供“全部智能体”；已删除或不可调用 Agent 只为已有 Session 保留历史筛选入口，不能新建 Draft。
- 点击“新建会话”直接打开所选 Agent 的空白或已有本地 Draft，不展示初始消息表单，也不在首条消息被接受前创建或列出 Session；从外部平台视图发起时自动切回“本平台”。
- External Session 在 Hub 管理台可查看完整消息和活动历史，但不能发送消息、立即引导或停止 Turn；对应 Hub console API 同样拒绝写操作，匹配来源的外部集成接口不受影响。
- 会话输入框使用 `Enter` 发送、`Shift+Enter` 换行，并在 2 到 5 行间自动增高；重进多 Turn Session 后仍展示所有 Run 的回答。
- `/runtimes` 能看到 Runtime 身份、状态、labels 和 heartbeat 时间。
- Integration App 后端用 HTTP Basic 为一个可信外部用户签发 Widget credential；签发本身不创建 Session，首条消息才原子创建 Hub/Integration Session 和 Run，并把外部用户快照交给 Runtime/Pi。
- widget 能创建或选择受完整 external scope 约束的 Session，续期后继续相同 Session，发送消息并看到 fake provider 回复；凭证轮换、失败的 JWT exchange 和同 token `session-select` 都不丢草稿或解除 pending 提交锁。
- Widget history 默认关闭。开启后可列出、切换并继续同 scope 历史；关闭后不展示列表，但当前标签页刷新可凭 `sessionStorage` 中的精确 Session ID 恢复当前对话和草稿。
- 管理员可在普通 Integration App 上关闭登录要求并配置精确嵌入 Origin；公开 Widget 固定一个 Agent、关闭历史，以 App-scoped visitor key 轮换短期 credential，首条消息才创建 `public_widget` Session。
- Agent 可选择可调用工具，Integration App 只能配置其关联 Agent 工具的公共子集。Run claim 使用两者交集；公开 Widget 进一步强制只读文件工具、无网络且无 MCP。
- 宿主页面能通过 `postMessage` 提交 widget 消息，并收到 Run started 和 Run event 通知；切换逻辑会话只停止旧 SSE 的前端展示，不停止后台 Run，也不把迟到事件写入新会话。
- 同一 Session 的后续消息复用其 Workspace 和 Native Session；不同 Session 的 Workspace 和 Execution Engine state 隔离。
- 后端、Runtime、前端构建与测试命令通过。

## 测试计划

- `qa/features.json` 是 V1 可验收行为与自动化证据的权威目录；`docs/qa-spec.md` 定义覆盖层级、场景隔离、诊断和 secret 安全契约。
- 每个 OpenAPI operation、管理台页面和关键 Widget/Runtime 协议都必须映射到稳定 Feature ID；场景通过 manifest 的 `covers` 声明自己直接验证的行为。
- 每个核心功能域至少保留一条真实 Compose API 或 Browser QA 链路；并发、持久化约束和协议状态机可继续由 Rust 测试承担，不在浏览器重复所有分支。
- 后端：覆盖凭据脱敏、Session/origin 授权、消息顺序、Run 关联和数据库迁移。
- Runtime：覆盖 Session 目录隔离、Native Session 多 Turn 继续、ownership fencing 和 fake Pi RPC 事件。
- Frontend：TypeScript 构建校验。
- 浏览器：覆盖登录、Session-first 导航、Agent/Session 创建、继续对话、Integration App、Markdown 编辑、Skill/API Key/Runtime/Administration 管理，以及 Widget 签发/续期/历史/消息，同时验证 desktop 与 390px。
