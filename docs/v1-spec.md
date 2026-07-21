# Agent Hub V1 骨架 Spec

## 范围

V1 保留以下可浏览器验证的产品链路；其执行和存储边界已由 ADR-0012 及 `docs/session-runtime-spec.md` 更新为 Session 级：

1. 用户登录并获取 Hub 登录会话。
2. 创建 Agent，保存 Markdown instructions、visibility、owner、managed Skills、MCP、Runtime 约束、默认 Model Connection、reasoning effort 和 Codex Subagent Definitions；sandbox policy 仍参与执行但不在管理台展示。
3. Runtime 通过管理员签发的一次性 Enrollment Token 建立身份，之后使用自己的可撤销 Runtime Credential heartbeat。
4. 用户选择 Agent 后进入未持久化的 Conversation Draft；首条消息原子创建 Hub Session、Message 和 Run，后续消息继续该 Session，且每条消息独立持久化。
5. Runtime 获得 Session 的排他 ownership generation，在该 Session 独立的 `workspace/` 和 Codex 目录中继续同一个 native Codex Thread。
6. Hub 保存消息、Run 和 native Item 映射事件，并通过 SSE 推送给管理台和 widget。
7. 管理台以 Session 为登录后默认页和第一导航，使用对话主视图展示消息、SSE 回复、按时间线折叠的 Codex 可读活动与历史状态；Hub 内部状态、用量和 delta 事件不直接展示。
8. Widget iframe 使用 embed token 访问其来源范围内的 Session，也支持宿主通过 `postMessage` 选择 Session 和提交消息。
9. Session 离线时由 Runtime 生成最小 `tar.zst` Bundle，经 Hub 流式写入对象存储；恢复也只经过 Hub。
10. Integration App 统一 OAuth、外部 Session API 和 Widget，可委托多个 Agent，并通过应用级或用户级 Application Token 使用显式 Agent scopes。
11. Automation、Skill、API Key、Runtime 和 Administration 均以列表或专用 Tab 为主体，新建/编辑表单只在点击操作按钮后打开。
12. Model Connection 以独立一级菜单管理 Global/Personal Responses 连接、系统默认模型和不可删减的 Token usage/error ledgers；Runtime 只通过 Hub 透明代理访问 provider。

## 兼容边界

- 既有 console、widget、integration 和 automation 用户链路继续可用；迁移只把它们接到统一 Session 生命周期。
- Hub-native Session 不伪造外部来源；外部 Session 固定绑定创建它的 External Platform、Tenant 和 Identity。
- Run 仍是调度、重试和审计记录，但不拥有 Workspace、Codex 目录、Thread 或 app-server 进程。
- Runtime 池属于管理员接入的可信基础设施；用户执行数据仍以 owner、Agent 权限、Session 隔离、MCP allowlist 和 sandbox 共同约束。

## 验收标准

- `docker compose up` 使用根目录 `compose.yml` 启动生产 Hub；可选的同机 Runtime 通过 `runtime` profile 启动。
- `docker compose -f compose.dev.yml up` 启动 PostgreSQL、包含前端静态资源的 Hub backend、fake provider 和 runtime 的完整开发环境。
- 用户能登录管理台、创建 Agent、进入空白 Conversation Draft，并从主对话输入框发送首条消息创建 Session 和启动 Turn，看到关联 Run 从 pending/running 进入终态。
- 登录和根路径默认进入 Session 页，可按 Hub-native/External Platform 来源筛选；选择 Agent 新建对话后直接进入空白主对话区，不展示初始消息表单。
- 会话输入框使用 `Enter` 发送、`Shift+Enter` 换行，并在 2 到 5 行间自动增高；重进多 Turn Session 后仍展示所有 Run 的回答。
- `/runtimes` 能看到 Runtime 身份、状态、labels 和 heartbeat 时间。
- widget 能创建或选择受其 origin 约束的 Session，发送消息并看到 fake Codex 回复。
- 宿主页面能通过 `postMessage` 提交 widget 消息，并收到 Run started 和 Run event 通知。
- 同一 Session 的后续消息复用其 Workspace 和 Codex Thread；不同 Session 的文件和 Codex 状态隔离。
- 后端、Runtime、前端构建与测试命令通过。

## 测试计划

- `qa/features.json` 是 V1 可验收行为与自动化证据的权威目录；`docs/qa-spec.md` 定义覆盖层级、场景隔离、诊断和 secret 安全契约。
- 每个 OpenAPI operation、管理台页面和关键 Widget/Runtime 协议都必须映射到稳定 Feature ID；场景通过 manifest 的 `covers` 声明自己直接验证的行为。
- 每个核心功能域至少保留一条真实 Compose API 或 Browser QA 链路；并发、持久化约束和协议状态机可继续由 Rust 测试承担，不在浏览器重复所有分支。
- 后端：覆盖凭据脱敏、Session/origin 授权、消息顺序、Run 关联和数据库迁移。
- Runtime：覆盖 Session 目录隔离、native Thread 多 Turn 继续、ownership fencing 和 fake app-server 事件。
- Frontend：TypeScript 构建校验。
- 浏览器：覆盖登录、Session-first 导航、Agent/Session 创建、继续对话、Integration App、Markdown 编辑、Skill/API Key/Runtime/Administration 管理和 Widget 消息，同时验证 desktop 与 390px。
