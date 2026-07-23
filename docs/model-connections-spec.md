# Model API Connections and Token Usage Spec

架构决策见 `docs/adr/0029-separate-model-api-connections-from-agent-model-settings.md`；协议转换边界见 `docs/model-proxy-spec.md`。

## 范围与术语

1. Model API Connection 是可复用的 provider 访问配置，只保存显示名称、Global/Personal 作用域、服务根地址、加密 API Key、单一 API Type、Allowed Model ID 列表、启停状态和时间戳。
2. Allowed Model ID 是连接内精确匹配的 provider `model` 字符串，不是独立 Model 实体。一个连接可以开放多个 Model ID，不为每个 Model ID 复制地址和密钥。
3. Agent Model Selection 是一个 `connection_id + model_id`；Agent Model Settings 保存 Codex 行为和协议专属请求设置。连接不保存调用参数。
4. API Type 只允许 `openai_responses`、`openai_chat_completions` 和 `anthropic_messages`。Runtime 始终使用 Responses；Hub 经内部 Model Gateway 透明转发或转换。
5. V1 直接使用本文件定义的最终 schema 和 API。开发、测试环境重建数据库，不回填旧连接字段，不保留旧 Run，不接受 connection-ID proxy header，也不提供旧 DTO alias。

## Model API Connection

规范化发生在创建和更新 API 边界：

- 输入必须是数组；每项先执行 Unicode 首尾空白裁剪。
- 裁剪后的 ID 长度为 1 到 255 个 Unicode scalar value，且不得包含 control character。
- ID 区分大小写。按规范化后的完整字符串去重，保留第一次出现顺序。
- 去重后必须保留 1 到 256 项。数据库同时保证连接不能提交空 allowlist。
- 请求体中的 `model` 必须与 Run binding 的 Model ID 字节级一致；Hub 不做 alias、模糊匹配或大小写折叠。

Canonical read DTO：

```json
{
  "id": "uuid",
  "name": "Provider A",
  "scope": "global",
  "owner_id": null,
  "base_url": "https://provider.example/api",
  "api_type": "openai_responses",
  "allowed_model_ids": ["gpt-5.6", "gpt-5.6-mini"],
  "status": "enabled",
  "has_api_key": true,
  "created_at": "timestamp",
  "updated_at": "timestamp"
}
```

- `POST /api/model-connections` 创建连接；API Key 和非空 allowlist 必填。
- `PUT /api/model-connections/{id}` 更新名称、地址、API Type、allowlist 和可选的新 Key。未提交 Key 时保留现有 ciphertext。作用域和 owner 不可变。
- `PUT ...?force=true` 允许移除仍被选择的 Model ID，或修改仍被选择连接的 API Type，并执行下文的原子清理。未使用 `force=true` 时返回 `409 Conflict` 和不含 secret 的引用摘要。
- `POST /api/model-connections/{id}/test` 接收 `{ "model_id": "...", "message": "hi" }`，只测试该连接当前 allowlist 中的精确 Model ID。Hub 发送一次非流式请求并返回模型文本、HTTP 状态与读取完整响应正文所用的毫秒数；测试输入和输出不写入 usage/error ledger，响应中的合法 usage 仍按既有规则记账。
- status、普通删除和 force-delete 沿用独立 action。读取、列表、OpenAPI、错误和日志永不返回明文 Key。
- `GET /api/model-connections/options` 返回扁平选择项：一个允许 Model ID 对应一项，包含 connection ID/name/scope/API Type/model ID/status。Agent 表单不自行组合连接与模型。

Personal 连接只能分配给同一 owner 的 Agent；Global 连接可分配给所有 Agent。共享 Agent 始终使用 Agent owner 保存的选择，调用者不能替换。Administrator 管理 Global 连接；普通用户管理自己的 Personal 连接；`admin` 不得查看或修改 `super_admin` 的 Personal 连接。

Base URL 不包含 `/v1`，可以包含业务路径。Global 和 Personal URL 都不限制公网、内网、loopback、link-local、metadata、DNS、redirect 或明文 HTTP；HTTPS 仍执行标准证书和 hostname 校验，不提供跳过校验。

## Agent Model Selection

```json
{
  "connection_id": "uuid",
  "model_id": "gpt-5.6"
}
```

- pair 必须同时存在或整体为 `null`。数据库和 API 都拒绝半个 selection。
- 连接必须符合 Agent owner 的 Global/Personal scope，Model ID 必须在当前 allowlist 中。
- System Default Model Selection 只能引用 enabled Global 连接及其一个 Allowed Model ID。创建 Agent 时复制 pair；以后修改默认值不改变已有 Agent。
- 没有 selection 的 Agent 仍可保存和查看，但属于 model-unconfigured，不能启动新 Turn。
- Codex Subagent Definition 不提交 selection 时继承 Agent pair；显式提交时必须是另一个完整、可授权 pair。

## Agent Model Settings

根 Agent 保存一个完整 `model_settings` 对象。枚举的 `default` 和数值/字符串的 `null` 表示保持 Codex/provider 自动行为：

| 字段 | 自动值 | Codex 原生配置 | 约束 |
| --- | --- | --- | --- |
| `reasoning_effort` | `default` | `model_reasoning_effort` | `default`、`none`、`minimal`、`low`、`medium`、`high`、`xhigh`、`max`、`ultra` |
| `reasoning_summary` | `default` | `model_reasoning_summary` | `default`、`auto`、`concise`、`detailed`、`none` |
| `verbosity` | `default` | `model_verbosity` | `default`、`low`、`medium`、`high` |
| `context_window_tokens` | `null` | `model_context_window` | 正整数 |
| `auto_compact_token_limit` | `null` | `model_auto_compact_token_limit` | 正整数，不大于 context window |
| `reasoning_summary_support` | `auto` | `model_supports_reasoning_summaries` | `auto`、`supported`、`unsupported` |
| `service_tier` | `null` | `service_tier` | trim 后 1 到 64 字符 |
| `request_max_retries` | `null` | provider `request_max_retries` | `0..100` |
| `stream_max_retries` | `null` | provider `stream_max_retries` | `0..100` |
| `stream_idle_timeout_ms` | `null` | provider `stream_idle_timeout_ms` | 正整数毫秒 |

`request_settings` 是与有效 API Type 一致的 tagged object：

| API Type | Request Settings | Gateway 行为 |
| --- | --- | --- |
| `openai_responses` | 只允许 `{ "protocol": "openai_responses" }` | 不注入参数，request/response body 保持字节透明 |
| `openai_chat_completions` | nullable `temperature` (`0..2`)、`top_p` (`0..1`)、`max_completion_tokens`（正整数） | 转换后仅覆盖非 `null` 字段 |
| `anthropic_messages` | nullable `temperature` (`0..1`) 或 `top_p` (`0..1`) 至多一个；nullable `max_tokens`（正整数） | 转换后仅覆盖非 `null` 字段 |

Subagent 使用 `model_settings_override`：

- 字段缺失表示继承 Agent；枚举可以显式提交 `default`，nullable 数值/字符串可以显式提交 `null`，从而覆盖 Agent 并恢复自动行为。
- 有具体值时覆盖 Agent。有效优先级逐字段为：Subagent override、Agent value、Codex/provider 自动行为。
- 未显式选择另一 pair 时，请求设置按字段继承且协议相同。显式选择不同 API Type 时，缺失的 `request_settings` 使用新 API Type 的全自动对象，不能继承不兼容协议字段。
- Runtime 只把 Codex 原生字段写入受控 TOML；协议专属 request settings 只进入 Hub 到 Gateway 的 envelope。

## 引用、删除和生效边界

- 普通 allowlist 更新在待移除 ID 仍被 System Default、Agent 或显式 Subagent selection 引用时返回 `409`。被引用连接的 API Type 变更以及普通删除同样返回 `409`，避免留下协议不匹配的 Agent settings。
- Force 更新原子清除受移除 Model ID 或 API Type 变化影响的 System Default 和 Agent selection。受影响的显式 Subagent Definition 保留但设置为 disabled，原因是 `model_selection_removed`；不得静默改为继承。
- Force Delete 执行相同清理并删除 live secret/config。Agent、Subagent、Run、usage 和 error 的非 secret 历史仍可查看。
- 连接名称、allowlist、API Type、Agent selection 或 settings 的变化在下一 Run binding 生效；Run binding 不被修改。
- Base URL 或 API Key 轮换从下一次 provider request 生效。disabled/deleted 连接阻止下一次 request；已交给 Gateway 的流可以完成。
- 修改 allowlist 不使既有 Run binding 失效。该 binding 的 Model ID 已在 Run 开始时授权；Hub 仍要求 live 连接存在且 enabled。

## Run Model Binding 与调用链

每个 Run 为主 Agent和每个不同的显式 Subagent 配置建立不可变、无 secret 的 binding：

```json
{
  "id": "binding-uuid",
  "run_id": "run-uuid",
  "binding_key": "main",
  "model_connection_id": "connection-uuid",
  "connection_name_snapshot": "Provider A",
  "connection_scope_snapshot": "global",
  "model_id": "gpt-5.6",
  "api_type": "openai_responses",
  "model_settings": {}
}
```

1. Runtime claim 只包含 binding snapshots，不含 provider endpoint、Key 或 connection-level secret。
2. Runtime 为每个 binding 渲染受控 Codex provider，并通过 loopback proxy 发送 Run ID、binding ID 和 run-scoped token。
3. Hub 只接受 binding ID。它必须属于认证的 active Run，且 request `model` 等于 binding Model ID。
4. Hub 验证 Runtime、Run、Agent scope、binding snapshot 和 live connection enabled state；每次请求读取一次 live endpoint/ciphertext 并解密 Key。
5. Hub 使用 binding 的 API Type 和 effective request settings 调用 Gateway。两个 Agent 即使共享 connection/model，也由不同 binding ID 保留各自设置。
6. Gateway 不保存连接、Key、prompt、output、usage 或 error，不做 retry/fallback；Hub 是唯一 control plane 和历史账本权威。

## 密钥、用量和错误账本

- Hub 从 `HUB_MODEL_SECRET_KEY` 读取部署级对称主密钥，使用带完整性校验的对称加密和每条 secret 独立随机 nonce。缺失或格式错误时拒绝启动。
- 每个真实上游请求最多写一条 Model Token Usage。任意 terminal status 只要有合法 `usage` 就记录；失败、incomplete、cancelled、transport 或 protocol failure 同时写 Model Call Error；无 usage 的失败不增加 Token 总量。
- usage 保存 input/output/total/cached/reasoning tokens。cached 是 input 子集，reasoning 是 output 子集，不重复累加。
- usage/error 快照保存 connection name/scope、API Type、Model ID、有效 request settings、Agent 和发起 subject；不区分主/子 Agent，不保存 prompt、output、raw body、headers 或 credential。
- 两类 ledger 使用 `TIMESTAMPTZ(3)`，不可删减。删除 User 后匿名化身份关联；删除 Agent、Session、Run 或 Model API Connection 不改变历史总量。
- 普通用户可查看自己的 usage/error 和自有 Agent 汇总；共享 Agent 的 owner 看不到其他调用者身份。`admin` 可看非 `super_admin` 范围；`super_admin` 可看全平台。

## API、OpenAPI 和管理台

- OpenAPI 的 create/update/read/test/options/System Default/Agent execution schemas 使用本文件的 final V1 字段；单值 `model_id`、`parameters`、`request_parameters` 不再属于 connection DTO。
- “模型”页以 Global/Personal Model API Connection 列表和 usage/error 为主体。连接 FormDialog 只展示名称、作用域、Base URL、API Key、API Type、Allowed Model IDs 和状态操作。
- Agent 新建/编辑表单选择扁平 connection/model option，并编辑完整 Model Settings；Subagent 子表单显示 inheritance source 和 effective value。
- 用量页默认当天，可选昨天、7/30/90 天、总共。前端计算毫秒级半开区间 `[from_ms,to_ms)`；汇总覆盖完整范围且不受明细分页影响。
- 第一版不做独立 Model catalog、`/v1/models` 自动发现、alias、wildcard、价格、图表、quota、rate limit、fallback、CSV 或 secret 查询。

## 验收标准

- 序列化和 PostgreSQL 测试覆盖 allowlist 规范化、pair 完整性、scope、System Default、API-Type settings、Subagent 逐字段继承和 binding 不可变性。
- API 测试覆盖一个连接多个模型、扁平 options、精确测试、Key 不披露/轮换、引用冲突、force 清理、model-unconfigured、admin/super-admin 和 Personal owner 边界。
- Runtime/Hub 测试证明只使用 binding ID，并能区分共享 connection/model 但 settings 不同的两个 binding；任何 connection-ID-only 请求 fail closed。
- Gateway 测试保持 Responses 字节透明，并验证 Chat/Messages 转换只合并 binding 的有效 request settings。
- usage/error 在 live connection 删除后仍按快照汇总，且所有日志、OpenAPI、Runtime 文件和 QA artifact 不包含 API Key。
- 管理台在 desktop 和 390px 验证最小连接表单、allowlist、Agent/Subagent selection/settings、System Default、冲突/force 和历史用量，无 console/network 异常或横向溢出。
