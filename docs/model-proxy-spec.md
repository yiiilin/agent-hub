# Model Proxy Spec

## 功能范围

本文件只定义传输边界；Model Connection、权限、密钥、用量和 UI 的权威契约见 `docs/model-connections-spec.md`。

1. Runtime 为每个在线 Session 保持一个 loopback proxy，并在当前 Run 切换时原子更新 run-scoped model proxy token。
2. Codex 原生 provider 和自定义子 Agent provider 都请求 loopback proxy；Runtime 添加 Run ID、Model Connection ID 和 Hub token 后转发到 `POST /api/runtime/model-proxy/v1/responses`。
3. Hub 验证 active Run/Runtime、Agent connection scope、enabled state 和 Model ID，每次请求从 PostgreSQL 读取一次连接并解密 API Key。
4. Hub 追加固定 `/v1/responses` 上游路径，替换 `Authorization`，其余 Responses request 和 streamed response 保持透明。
5. Hub 旁路解析 terminal JSON/SSE：存在有效 `usage` 时写入 immutable usage ledger；失败或协议错误写入 sanitized error ledger。解析不能延迟首个下游 chunk。
6. Runtime 不接收 API Key，不直连 provider，也不从旧环境变量 fallback。

## Header 和流式边界

- 请求保留 query、原始 body bytes 和安全 end-to-end headers；剥离 Cookie、Host、Content-Length、Connection 声明的 headers、标准 hop-by-hop headers 和 Agent Hub 内部认证/路由 headers。
- 上游认证固定为 Model Connection 的 `Authorization: Bearer <API Key>`。
- 响应保留 status、Content-Type、request tracing 等非敏感 end-to-end headers，并剥离 hop-by-hop、Content-Length 和 provider credential-like headers。
- SSE chunk 到达即向 Runtime 输出，同时用有界 parser 观察 terminal event；不得为统计完整缓冲响应。
- upstream header timeout 在发送下游 headers 前映射为 gateway error；body 中途失败终止 downstream stream 并写错误记录，不能伪造 completed event。

## 不做范围

- 不转换 Chat Completions，不改写 request `model`，不注入 prompt，不重排 SSE event。
- 不支持 Runtime direct fallback、provider key 下发、任意 connection-level custom headers 或本地估算 Token。
- 不通过应用层限制用户配置的 HTTP/HTTPS endpoint 地址。

## 验收标准

- 无效 token、非 active Run、offline Runtime、越权/disabled/deleted connection、Model ID 不匹配和非 `responses` path 全部 fail closed。
- 原始 JSON bytes、query、允许 headers、非成功 status、SSE first chunk 和 idle/body timeout 均有聚焦测试。
- API Key 只出现在 Hub 到 provider 的请求；Runtime、Run event、usage/error、日志和响应不包含 secret。
- completed without usage 作为协议错误；任意 terminal status 带 usage 时仍记录该 usage，错误状态同时记录 Model Call Error。
