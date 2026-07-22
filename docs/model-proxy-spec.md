# Model Proxy Spec

## 功能范围

本文件只定义传输边界；Model Connection、权限、密钥、用量和 UI 的权威契约见 `docs/model-connections-spec.md`。

1. Runtime 为每个在线 Session 保持一个 loopback proxy，并在当前 Run 切换时原子更新 run-scoped model proxy token。
2. Codex 原生 provider 和自定义子 Agent provider 都请求 loopback proxy；Runtime 添加 Run ID、Model Connection ID 和 Hub token 后转发到 `POST /api/runtime/model-proxy/v1/responses`。
3. Hub 验证 active Run/Runtime、Agent connection scope、enabled state、Model ID 和当前 Run 固定的 Upstream Protocol，每次请求从 PostgreSQL 读取一次连接并解密 API Key。
4. Hub 把该次请求的协议、协议专属请求参数、服务根地址、API Key、query、安全 headers 和原始 Responses body 封装为一次性 envelope，使用内部 Bearer Token 调用 Model Gateway。Gateway 不查询 Agent Hub 数据库，也不持久化 envelope。
5. Gateway 根据协议透明转发或转换请求，并返回 Responses JSON/SSE。Hub 旁路解析 terminal response：存在有效 `usage` 时写入 immutable usage ledger；失败或协议错误写入 sanitized error ledger。解析不能延迟首个下游 chunk。
6. Runtime 不接收 API Key、不直连 provider、不访问 Model Gateway，也不从旧环境变量 fallback。

## 支持协议矩阵

| Runtime/Hub 入口 | Model Connection `upstream_protocol` | Provider 请求 | 返回给 Hub/Runtime | V1 状态 |
| --- | --- | --- | --- | --- |
| Responses JSON/SSE | `openai_responses` | `<base>/v1/responses`，`Authorization: Bearer <key>` | 原始 status、允许 headers 和 body bytes；SSE chunk 顺序不变 | 支持，默认值 |
| Responses JSON/SSE | `openai_chat_completions` | `<base>/v1/chat/completions`，`Authorization: Bearer <key>` | Bifrost 规范化的 Responses JSON/SSE，包含 terminal status、tool/reasoning item 和 usage | 支持；只转换可无损表达的请求 |
| Responses JSON/SSE | `anthropic_messages` | `<base>/v1/messages`，`x-api-key` 和固定 `anthropic-version` | Bifrost 规范化的 Responses JSON/SSE，包含 terminal status、tool/reasoning item 和 usage | 支持 |

服务根地址不带 `/v1`，但可包含业务路径。Gateway 去除末尾 `/` 后追加矩阵中的固定路径，并原样保留 Hub 已验证的 query。

## 连接配置与请求边界

- Model Connection 的 `parameters`（reasoning、summary、verbosity、context、compaction、service tier 和 provider retry/idle）由 Runtime 写入 Codex 原生 TOML；它们不进入 Hub 到 Gateway 的请求级 envelope。
- `request_parameters` 进入 envelope 且其 `protocol` 必须与连接协议相同。Responses 协议没有覆盖字段；Chat 可覆盖 `temperature`、`top_p`、`max_completion_tokens`；Anthropic 可覆盖 `temperature` 或 `top_p` 以及 `max_tokens`。
- Codex 根据原生配置产生的 Responses 请求体仍按既有链路传输。Hub 只验证请求中的 Model ID，OpenAI Responses fast path 保持原始 body bytes；Chat 和 Anthropic 路径在验证可表示性后转换，并只合并非 `null` 的协议专属覆盖。
- 转换无法无损表达的 Responses 字段、历史项、工具或采样冲突必须在上游调用前以 `unsupported_protocol_feature` 返回；Gateway 不静默丢弃字段。跨协议历史只接受严格白名单中的 message、function call/result、reasoning、refusal 与其可移植内容字段；例如 item `id`、`status`、`phase`、未知 item 字段和 `output_text.annotations`/`logprobs` 一律拒绝。除 `request_parameters` 外，Hub 和 Gateway 不向请求体注入采样、输出限制、`tool_choice` 或 `parallel_tool_calls`。
- `request_max_retries`、`stream_max_retries` 和 `stream_idle_timeout_ms` 属于 Codex provider 客户端行为。Gateway 继续不重试、不 fallback；一次到达 Gateway 的请求只产生一次上游调用。

## Header 和流式边界

- Runtime 请求保留 query、原始 body bytes 和安全 end-to-end headers；Hub 剥离 Cookie、Host、Content-Length、Connection 声明的 headers、标准 hop-by-hop headers 和 Agent Hub 内部认证/路由 headers。
- Hub 到 Gateway 使用独立内部 Bearer Token；Gateway 按协议矩阵重写 provider 认证，Runtime 提交的认证值不能到达 provider。
- `openai_responses` 保留 provider status、Content-Type、request tracing 等非敏感 end-to-end headers，并剥离 hop-by-hop、Content-Length、Set-Cookie 和 credential-like headers。`openai_chat_completions` 与 `anthropic_messages` 返回规范化 Responses status、headers 和 body。
- OpenAI fast path 禁用 Go transport 的自动压缩协商和解压；provider 返回的 `Content-Encoding` 及压缩 body bytes 原样保留。
- OpenAI Responses SSE chunk 到达即向 Runtime 输出；Chat Completions 与 Anthropic SSE event 到达即转换并输出 Responses event。Hub 同时用有界 parser 观察 terminal event，不得为统计完整缓冲响应。
- upstream header timeout 在发送下游 headers 前映射为 gateway error；body 中途失败终止 downstream stream 并写错误记录，不能伪造 completed event。
- Runtime/Hub 下游断开必须取消 Gateway 请求；转换路径还必须关闭该请求对应的 provider TCP 连接。

## 不做范围

- 不提供 Runtime/Hub Chat Completions 入口；所有入口仍为 Responses。除协议转换和连接 `request_parameters` 外，不改写 request `model`，也不注入 prompt、采样、输出限制或工具选择字段。
- 不支持 Runtime direct fallback、provider key 下发、任意 connection-level custom headers 或本地估算 Token。
- 不通过应用层限制用户配置的 HTTP/HTTPS endpoint 地址。
- Gateway 不做自动 retry、fallback、持久化、缓存、provider CRUD、usage/error 记账或 prompt/output 日志。

## Provider 信任边界

- Model Connection 所有者负责信任所配置的 provider。Gateway 必须把 API Key 发给该 provider，且 OpenAI fast path 为保持 JSON/SSE 字节透明，不检查或改写 provider response body。
- Hub 和 Gateway 保证自身生成的错误、日志、账本、Run event 以及转发的 response headers 不包含 credential。provider 可以在 body 中返回任意编码的数据，通用透明代理无法可靠判定其中是否包含 prompt、output 或 provider 已知的 credential；因此“secret 不返回”不对恶意或失陷 provider 的 body 作虚假保证。
- Global Model Connection 的 Administrator 与 Personal Model Connection 的 owner 分别承担 provider 选择风险。需要把 provider 视为不受信输入时，必须在外部 egress/content policy 层取消字节透明并实施专用检查，不属于 V1 Gateway 范围。

## 验收标准

- 无效 token、非 active Run、offline Runtime、越权/disabled/deleted connection、Model ID 不匹配和非 `responses` path 全部 fail closed。
- OpenAI Responses 原始 JSON bytes、query、允许 headers、非成功 status 和 SSE first chunk 保持透明；Chat Completions 与 Anthropic JSON/SSE 转换保留 terminal usage、tool/reasoning 语义，并覆盖 idle/body timeout、取消、参数合并和不可表示请求的上游零调用。
- API Key 只由 Hub 到 Gateway 的请求级 envelope 和 Gateway 到所选 provider 的认证 header 主动发送；Runtime 配置、Run event、usage/error、Gateway/Hub 生成的错误、日志和 response headers 不包含 secret。provider body 适用上述信任边界。
- completed without usage 作为协议错误；任意 terminal status 带 usage 时仍记录该 usage，错误状态同时记录 Model Call Error。
- Run Model Connection snapshot、Model Token Usage 和 Model Call Error 都保存请求生效时的三种协议与协议专属请求参数，后续修改连接不改写历史。
- 详细 Model Connection `parameters` 只改变 Runtime 生成的 Codex 配置；OpenAI Responses 透明转发测试必须证明 Gateway 不因这些参数或空的 Responses 请求参数改写 body 或增加 retry。
