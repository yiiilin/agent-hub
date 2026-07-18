# Integration App 与外部 Session Spec

## 范围

1. Integration App 归属一个 Hub User，并固定关联一个 External Platform 和其下启用的 Authentication Channel。
2. 应用管理者可关联自己当前 `can_invoke` 的多个 Agent；关联即向应用委托该 Agent 权限。
3. 应用支持 OAuth `client_credentials` 和 `authorization_code` grant，访问令牌只能访问 `/api/integrations/*`、`/api/oauth/userinfo` 和允许的 Widget 换取入口，不能进入 Hub 控制面。
4. Agent scope 固定为 `agent:<uuid>`，请求的 scope 必须全部合法、已关联且在使用时仍然有效；Agent scope 没有默认值。
5. `authorization_code` token 代表登录 Hub User，默认 profile scopes 为 `profile email external_profile`；Agent scope 同时受该用户当前 Agent 访问权限约束。
6. authorize 请求携带 `external_user_id` 和 `tenant_id`，Hub 验证该 External Identity 已绑定当前 Hub User。`userinfo` 按 token scopes 返回 Hub 用户资料和该应用平台下的外部身份资料。
7. `client_credentials` token 代表应用，可为多个外部用户创建和继续 Session；每个 Session 仍固定保存自己的 platform、tenant 和 External Identity origin。
8. 每个 Integration Session 固定归属其 Integration App 和 Agent。应用级 token 可管理该应用为多个外部身份创建的 Session；用户级 token 只能管理自己的 Session。
9. 解除 Agent 关联、应用管理者失去 `can_invoke`、用户失去 Agent 权限或 Agent 被删除时，已签发 token 对该 Agent 立即失效。
10. Integration 消息、Steering Message、tool request/result、SSE 和 native Thread 延续仍遵守通用 Session 顺序与 Turn 边界。
11. Integration App 中每个已关联 Agent 单独生成短期 Widget 地址，不存在跨 Agent 的通用嵌入凭据。

## 非目标

- 不做 consent 审批页、refresh token、二进制上传、外链抓取、OCR 或病毒扫描。
- 不将 Application Token 接入 `require_user()`，不允许应用凭据访问控制面 API。
- 不允许跨 Integration App、External Platform 或未委托 Agent 访问 Session。
- 本轮不提供 Integration App 删除，只提供新建、编辑和 client secret 轮换。
- 不把 tool result 建模为新 Codex Thread，不合并或覆盖原消息历史。

## 验收标准

- 管理台以 Integration App 表格为主体，新建和编辑使用子表单，client secret 仅在创建或轮换响应中显示一次。
- 一个应用可关联多个 Agent，两种 grant 只能换取请求且有效的 Agent scopes。
- 用户级 token 可按 profile scopes 读取 `userinfo`；应用级 token 不得伪造 Hub 用户资料。
- 应用级 token 可为两个外部用户分别创建 Session，两个 Session 的 origin 互不混同。
- Bearer token 访问 `/api/auth/me` 或 `/api/agents` 被拒绝。
- 发送需要工具的消息后出现 `tool_request`；提交 result 后在同一 Session/Thread 中得到结果。
- 解除关联或权限收回后，旧 token 的新请求和存量 SSE 都不再获得该 Agent 数据。

## 测试计划

- Rust：覆盖 secret hash、两种 grant、scope 子集、userinfo、应用/用户 principal、权限即时失效、origin 隔离、消息顺序和 tool result 继续。
- TypeScript：前端构建通过。
- 浏览器：覆盖 Integration App 新建/编辑/轮换、Agent 关联、OAuth grants、Session/message/SSE、Widget 地址和权限收回。
