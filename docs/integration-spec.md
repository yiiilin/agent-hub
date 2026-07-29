# Integration App、Browser SDK 与 Client Tool Spec

## 文档边界

- 本文定义 Integration App、Client Access Credential、Browser SDK 和 Client Tool 的规范契约与验收边界。
- 面向接入方的安装、可信后端授权、TypeScript、React、匿名恢复、错误处理和安全示例见 [Browser Client SDK 接入指南](./client-sdk-guide.md)。
- `@agent-hub/client` 的包级快速开始、public exports 和本地构建命令见 [TypeScript SDK README](../sdk/typescript/README.md)。示例不扩展本文契约；若示例与 public declarations 不一致，以实际安装制品的 declarations 和本文为准。

## 范围

### Integration App 与身份

1. Integration App 归属一个 Hub User，并固定关联一个 External Platform 和其下启用的 Authentication Channel。
2. 应用管理者可关联自己当前 `can_invoke` 的多个 Agent；关联即向应用委托该 Agent 权限。
3. 应用支持 OAuth `client_credentials` 和 `authorization_code` grant。Application Token 只能访问 `/api/integrations/*`、`/api/oauth/userinfo` 和允许的 Client Access Credential 签发入口，不能进入 Hub 控制面。
4. Agent scope 固定为 `agent:<uuid>`，请求的 scope 必须合法、已关联且在每次使用时仍然有效；Agent scope 没有默认值。
5. `authorization_code` token 代表登录 Hub User；`client_credentials` token 代表应用。两者创建的 External Session 都固定保存自己的 External Platform、Tenant 和 External Identity origin。
6. 解除 Agent 关联、应用管理者失去 `can_invoke`、用户失去 Agent 权限或 Agent 被删除时，已签发凭证不能发起新的 Agent 操作。

### Client Access Credential

7. 有后端的应用在用户登录授权时，以 `client_id`/`client_secret`、可信外部用户资料、明确的 `agent_id`、浏览器提供的 `client_instance_id` 和完整 Client Tool 定义向 Hub 申请 Client Access Credential。`client_secret` 永不进入浏览器。
8. Client Access Credential 是 15 分钟有效的随机 opaque Token。Hub 只保存其 hash，并在现有 credential 记录的 JSON 字段中保存工具定义；Token 不使用 JWT，也不携带工具 Schema。Hub 重启不使其失效。
9. Credential 固定绑定 Integration App、Agent、External Platform、External Tenant、External Identity、Hub User 和一个 Client Instance，但不绑定单个 Session。一张有效凭证可同时操作其完整 scope 下的多个 Session。
10. Client Instance 以浏览器标签页为单位。SDK 用 `sessionStorage` 保存其随机 ID，使当前标签页刷新后保持不变；并用 `BroadcastChannel` 探测 `window.open` 克隆出的仍在使用的 ID，为新 JS realm 重新生成 ID。同一标签页中的多个 SDK Client 共享该 ID，新标签页获得独立 ID 和独立凭证。相同 Client Instance 的续期或重新授权原子替换旧 Token，不影响其他标签页。
11. SDK 在到期前直接向 Hub 续期。普通续期沿用原 Client Tool Grant；续期返回 `401` 时只调用一次应用提供的 `authorize` 回调。应用也可显式调用 `reauthorize()` 获取最新工具集合。在途 Run 始终保留创建时的工具快照。
12. V1 不提供独立手动撤销接口。Token 通过到期、同 Client Instance 轮换以及应用、用户、Agent 和 delegation 的实时状态校验失效。浏览器显式退出只清除内存 Token，旧 Token 最多继续有效 15 分钟。
13. SDK 不把 Credential 写入 `localStorage`、`sessionStorage` 或 IndexedDB。页面重新加载后，认证应用重新调用自己的后端授权；匿名应用使用本地 visitor key 重新获取 Credential。

### Session 与历史

14. Credential 签发不创建 Hub Session。SDK 的 `draft()` 仅存在于调用方内存；首条被接受的消息在一个事务内创建 Hub Session、Integration Session、Message 和 Run，并返回一个对外 `session_id`。
15. 认证 Credential 可列出和继续相同 App、Agent、Platform、Tenant 与 External Identity 范围内的多个 Session。`widget_history_enabled` 关闭时禁止列表发现，但持有精确 `session_id` 的页面仍可读取并继续该 Session。
16. 一张 Credential 可并发操作多个 Session；同一 Session 同时只允许一个活动 Turn。活动期间再次 `send()` 按通用 Session 规则立即引导当前 Turn，不创建并行 Turn，也不改变该 Run 的 Tool Executor 或工具快照。
17. 多个 Client Instance 可同时查看同一 Session。发起 Run 的 Client Instance 是该 Run 唯一的 Run Tool Executor；其他标签页或设备只能观察该 Run，不能提交其 Client Tool 结果。
18. 匿名应用不提供 Session 列表。App-scoped visitor key 和精确 `session_id` 保存在浏览器本地，用于重新获取 Credential 后恢复唯一的当前 Session；清除浏览器存储后形成新的匿名访客，不能发现旧 Session。
19. Session 消息、技术事件和工具调用按服务端事件顺序持久化。SDK 使用稳定 `client_message_key` 重试不确定的消息 POST，并以 SSE event sequence 断线续传，不自动重复发送已被接受的用户消息。

### Client Tool Grant 与 Run 快照

20. Client Tool 的协议形状为 `{ name, description, input_schema }`。`name` 只能包含字母、数字、`_` 和 `-`，长度为 1 到 64；同一 Grant 内名称唯一；最多 128 个工具；完整定义序列化后最多 256 KB；`input_schema` 必须是对象类型 JSON Schema。
21. 有后端的应用可在每次授权时提交完整动态工具定义。匿名应用不能从浏览器提交或改写工具，只能使用管理员在 Integration App 上预先保存的 Client Tool 定义。该配置使用现有 App 记录的 JSON 字段，不新增工具目录表。
22. Agent 的有效工具策略必须允许 `integration`，Client Tools 才会进入 Runtime。Runtime 使用隐藏命名空间避免 Client Tool 与 `read`、`bash`、`skill_exec` 等工具冲突；Client API、SDK 和应用只看到原始名称。
23. 每个新 Run 将当前 Credential 的 Client Tool Grant 固化为不可变 Run Tool Snapshot。Steering 和该批工具结果触发的 continuation 保留原 Snapshot；续期、重新授权和其他设备的 Grant 都不能改变它。
24. Runtime 将一个模型响应中的 Client Tool 调用记录为有序批次。SDK 按模型输出顺序串行执行；Hub 等整批调用都得到普通成功或失败结果后，只创建一个模型 continuation。工具等待期间已经排队的下一轮 Run 不可被 Runtime 领取；批次完成事务原子结束父 Run/Turn，并把该 pending Run 绑定为唯一 continuation 后才允许领取。
25. 每次调用具有稳定 `tool_call_id` 和 5 分钟硬期限。SDK 先在 IndexedDB journal 中持久记录，再向 Hub claim；只有 Hub 确认当前 Client Instance 是 Run Tool Executor 后才能调用应用处理函数。
26. SDK journal 按 Client Instance ID 隔离。服务端确认终态的记录保留 24 小时后清理；执行结果未知的记录保留到 Hub 明确返回终态。相同调用重新送达时，已完成调用只重交缓存结果，已进入执行但结果未知的调用绝不重跑。
27. 成功结果为 `{ "status": "success", "output": <JsonValue> }`；失败结果为 `{ "status": "error", "error": { "code": <string>, "message": <string>, "retryable": <boolean> } }`。SDK 捕获异常但不上传 JavaScript stack，也不自动重试。
28. Client Tool Result 的 UTF-8 JSON 最多 16,000 字节，超限明确失败且不截断。同一 `tool_call_id` 重交完全相同结果时幂等返回原记录；不同结果返回 `409 Conflict`。
29. 普通处理错误、`user_rejected` 和 `tool_handler_not_registered` 是终态结果，不阻止同批后续工具执行。handler 已执行但 Tool Result 提交结果未知、执行中断或 5 分钟超时会停止剩余批次并使当前 Turn 失败，不创建模型 continuation，不跨设备接管或重放。
30. Hub 和 SDK 不提供统一副作用确认框。接入应用的 handler 决定是否展示自己的确认 UI；外部写操作还必须在应用边界使用 `tool_call_id` 作为幂等键，因为 Hub 不能保证外部系统恰好执行一次。
31. Run Tool Snapshot、调用参数、状态和结果跟随 Session 历史保留，不因 Credential 到期或 Agent 删除而删除。Credential 记录可在不破坏匿名当前 Session 恢复映射的前提下清理。

### Browser SDK 与 Client API

32. `sdk/typescript/` 提供框架无关的 ESM 包 `@agent-hub/client` 和 TypeScript declarations。它只依赖浏览器标准 API，不依赖 React、Vue 或 Node runtime；V1 不提供 React Hooks、Node SDK 或独立 CDN 文件。
33. SDK 提供一个 `AgentHubClient`、Session 列表、现有 Session、延迟创建的 draft、消息分页、`subscribe()`、`send()`、`dispose()`、工具 handler 注册、`connectAnonymous()` 和 `reauthorize()`。浏览器只注册已授权名称对应的 handler，不能重定义工具描述或 Schema。
34. 认证初始化时，SDK 先生成 Client Instance ID，再调用接入方的 `authorize({ clientInstanceId })`。接入方后端通过语言无关 HTTP API 向 Hub 申请 Credential。接入文档必须同时提供后端 HTTP、原生 TypeScript 和 React 使用示例。
35. `/api/client/*` 是 SDK 与 Widget 共用的规范入口，覆盖 Credential、Session、消息、SSE、Run stop、Client Tool claim 和结果提交。现有 `/api/widget/*` 暂时保留为兼容别名，但不再作为主文档接口。
36. SDK 输出有类型的 `tool_request`、`tool_result`、timeout 和 error 事件，不把工具 JSON 混入 assistant 文本。Hub 管理台和 Widget 按消息顺序把它们显示为带工具名、状态和耗时的可折叠技术事件；外部应用自行决定展示方式。
37. `npm pack` 是交付验证的一部分，但本任务不发布 npm 包。当前 Widget 必须使用同一 SDK 核心，不维护第二套会话、续期或 Client Tool 状态机。

### Origin 与匿名应用

38. 认证 Integration App 的精确 Origin 白名单可选；未配置时允许任意 Origin，配置后按 `scheme://host[:port]` 精确匹配。
39. 匿名应用只能由 `admin` 或 `super_admin` 配置，必须关联恰好一个 Agent、关闭 history，并至少配置一个精确 Origin；任何浏览器请求都必须匹配。HTTP 和 HTTPS 均可配置，程序不强制生产 HTTPS，但文档必须说明 Token 窃听和 mixed-content 风险。
40. 匿名应用通过公开 `client_id`、匹配的 Origin、App-scoped visitor key 和 Client Instance ID获取 Credential，不提交应用 secret 或可信用户身份。它不能访问 External Identity、跨 Session 历史或其他 visitor 的当前 Session。
41. 匿名 Run 继续强制无网络、无 MCP、无 `bash`、写入和 `skill_exec`。允许的只读文件工具仍按 Agent/App 策略取交集；`integration` 只展开为管理员为该匿名应用配置的 Client Tools。
42. 外部应用直接使用 SDK 时由浏览器原生 `Origin` 参与校验。Hub 托管的跨 Origin iframe Widget 从已绑定的 `postMessage` `event.origin` 获取宿主 Origin；匿名首个请求优先读取 `ancestorOrigins[0]`，再回退到 `document.referrer`，并在同源 Hub 请求中使用 `X-Agent-Hub-Embedded-Origin` 转发。Hub 仅在浏览器同时发送 `Sec-Fetch-Site: same-origin` 时采用该转发值；外部 SDK 不设置此内部 header。

## 非目标

- 不把 `client_secret`、Application Token 或可信用户资料放入浏览器。
- 不允许浏览器动态声明工具名称、描述或 Schema。
- 不承诺 arbitrary Client Tool 的 exactly-once 执行，不自动跨设备接管或重放结果未知的调用。
- 不提供 Client Tool 服务端执行器、Node SDK、React Hooks、CDN 发布、通用确认 UI 或本轮 npm 发布。
- 不新增独立工具授权表，不把完整工具定义塞进 Token，也不改为 JWT。
- 不为匿名应用提供历史发现、可信 External Identity 或独立限流器。
- 不强制 HTTPS，不在未配置白名单的认证应用上限制 Origin；这些风险必须在接入文档中明确。

## 验收标准

- 管理台 Integration App 表单可维护匿名 Client Tool 定义；普通成员不能启用匿名访问，匿名应用缺少 Agent、Origin 或合法工具定义时保存失败。
- 应用后端可为可信外部用户和 Client Instance 签发带动态工具集合的 Credential；浏览器无法使用 `client_secret` 或自行扩大工具集合。
- 同一用户两个标签页获得不同 Client Instance 和 Token，可并发访问同一 Session；一个标签页续期不使另一个失效。
- 一张 Credential 可访问 scope 匹配的多个 Session；不同 App、Agent、Platform、Tenant、Identity 或匿名 visitor 不能越权。
- 首条消息前不创建 Session；消息不确定重试不会双写；活动 Turn 中的下一条消息立即 steer 而不创建并行 Turn。
- 新 Run 使用最新 Grant，旧 Run 在续期或重新授权后仍使用原 Snapshot；非 Executor 标签页不能 claim 或提交结果。
- 相同模型响应中的多个 Client Tools 串行执行并只产生一个 continuation。重复 claim/result、不同结果冲突、缺少 handler、用户拒绝、超限、断线、结果未知和 5 分钟超时均有确定状态与测试。
- SDK 刷新恢复、SSE cursor、IndexedDB journal 和 24 小时清理行为可验证；Token 不出现在浏览器持久存储、URL、日志或快照中。
- Widget 使用 SDK 核心，按消息顺序显示可折叠 Client Tool 技术事件；外部 SDK 示例可以驱动前端操作并继续模型输出。
- 配置 Origin 时严格匹配；认证应用空白名单允许任意 Origin；匿名应用必须配置且必须匹配。HTTP Origin 可保存并使用，同时 UI/文档展示风险提示。
- `@agent-hub/client` 构建、类型检查和 `npm pack` 通过，包内容不包含测试凭证或项目内部文件。

## 测试计划

- Rust 单元与数据库测试：工具定义校验、Credential scope/轮换、Client Instance 隔离、Origin 策略、Run Snapshot、claim 状态机、批量结果、幂等冲突、超时和历史保留。
- Runtime/Pi：隐藏名称映射、多工具批次、结构化成功/失败结果、单 continuation、旧 Snapshot 和 Native Session 延续。
- SDK：认证/匿名初始化、内存 Token、标签页 ID、IndexedDB journal、发送幂等、SSE 续传、续期 fallback、串行工具、缺失 handler、断线和清理。
- Browser：Integration App Client Tool CRUD、认证与匿名 Widget、两个标签页、history on/off、两轮对话、Client Tool 技术事件、desktop 和 390px、console/network diagnostics。
- 交付门禁：相关 Rust/TypeScript/Playwright 测试、SDK build 与 `npm pack`、一次 workspace/backend/frontend build，以及 OpenAPI、接入文档和 QA feature mapping 对齐。
