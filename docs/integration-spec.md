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
10. Integration 消息、Steering Message、tool request/result、SSE 和 Native Session 延续仍遵守通用 Session 顺序与 Turn 边界。
11. Integration App 后端使用自身 `client_id`/`client_secret` 和明确的 `agent_id`、`tenant_id`、`external_user_id` 调用 `POST /api/widget/access`；Hub 验证应用、Authentication Channel 和 Agent delegation 后签发 15 分钟 `ahw_` Widget Access Credential。应用 secret 不交给浏览器。
12. Widget Access Credential 固定绑定 Integration App、Agent、External Platform、External Tenant、External Identity 和 Hub User。签发 credential 时不创建 Hub Session；首条被接受的消息在一个事务内创建 Hub Session、Integration Session 和 Run。
13. 首条消息响应返回 `integration_session_id` 与 `hub_session_id`。继续同一对话必须显式回传这两个 ID；不带 ID 的下一条消息表示新建另一个对话。`client_message_key` 对一次用户提交保持稳定。
14. Widget 使用当前 `ahw_` 原位续期，`embed_sessions.id` 不变且旧 token 失效。普通续期只需当前 credential；Integration App 后端更新受信 profile 时还必须提供匹配应用的 HTTP Basic 凭据，更新只影响后续 Run 的外部用户快照。
15. 每个 Widget Run 固化其 `external_user` 快照并交给 Runtime/Pi，包含 external user ID、tenant、username、display name、email 和应用提供的 attributes；tool result 与 Steering fallback Run 保留同一快照。
16. `widget_history_enabled` 默认关闭。开启后历史列表只返回相同 Integration App、Agent、External Platform、External Tenant、External Identity 和 external user ID 的最多 100 个 Session；关闭后列表返回 403，但持有精确 Session ID 的当前页面仍可读取消息和事件以支持刷新恢复。
17. Widget 只在当前标签页的 `sessionStorage` 保存 credential、草稿和精确 Session ID。切换历史对话会停止旧 SSE 的前端展示，但不会向后台 Run 发送 stop；迟到的 POST/SSE 不得写入新选中的对话，全局提交锁在原 POST 结束后才释放。
18. `login_required` 是 Integration App 的普通配置属性，默认 `true`。只有 `admin` 或 `super_admin` 可将其关闭；公开 App 必须关联恰好一个 Agent、关闭历史，并配置至少一个不含通配符的精确 HTTP(S) Origin。Widget 页面用这些 Origin 生成 `frame-ancestors` CSP。
19. 公开 Widget 通过 `/widget?app=<client_id>` 打开，浏览器以 App-scoped visitor key 调用 `POST /api/widget/public/access`，不提交应用 secret 或用户身份。Hub 只保存 visitor key 的 hash，并按同一 App 与 visitor key 原位轮换 15 分钟 `ahp_` credential。
20. 公开 credential 签发时不创建 Hub Session；首条被接受的消息创建 `public_widget` Hub Session 和 Run，不创建 Integration Session。同一 App 与 visitor key 刷新后返回原 `hub_session_id`，可凭精确 ID 恢复当前对话，但历史列表始终返回 403，另一个 visitor key 不能访问该 Session。
21. Agent 明确选择可调用的内置文件、命令和 Integration 工具。Integration App 可不配置额外限制，也可配置其所有关联 Agent allowlist 的公共子集；Run 的有效工具是 Agent 与 App 策略的交集。公开 Widget 只允许 `read`、`grep`、`find`、`ls`，并强制 read-only、禁用网络和 MCP。

## 非目标

- 不做 consent 审批页、refresh token、二进制上传、外链抓取、OCR 或病毒扫描。
- 不将 Application Token 接入 `require_user()`，不允许应用凭据访问控制面 API。
- 不允许跨 Integration App、External Platform 或未委托 Agent 访问 Session。
- 本轮不提供 Integration App 删除，只提供新建、编辑和 client secret 轮换。
- 不把 tool result 建模为新 Native Session，不合并或覆盖原消息历史。
- 不签发永久 Widget token，不把 Integration App client secret 放入 iframe，不让浏览器直接更新受信外部用户资料。
- 历史关闭时不提供跨页面的 Session 发现；仅允许当前标签页凭精确 ID 恢复当前对话。
- 本版本不为公开 Widget 增加独立限流器；部署层仍可施加通用入口保护。

## 验收标准

- 管理台以 Integration App 表格为主体，新建和编辑使用子表单，client secret 仅在创建或轮换响应中显示一次。
- 一个应用可关联多个 Agent，两种 grant 只能换取请求且有效的 Agent scopes。
- 用户级 token 可按 profile scopes 读取 `userinfo`；应用级 token 不得伪造 Hub 用户资料。
- 应用级 token 可为两个外部用户分别创建 Session，两个 Session 的 origin 互不混同。
- Bearer token 访问 `/api/auth/me` 或 `/api/agents` 被拒绝。
- 发送需要工具的消息后出现 `tool_request`；提交 result 后在同一 Hub Session 和 Native Session 中得到结果。
- 解除关联或权限收回后，旧 token 的新请求和存量 SSE 都不再获得该 Agent 数据。
- 应用后端可为一个可信外部用户签发短期 Widget credential；自动续期不丢草稿、不解除 pending 提交锁，续期后的 token 可继续相同 Session。
- AI 的 Run context 包含签发时或经应用认证更新后的受信外部用户资料，失败请求或浏览器伪造不能更新该资料。
- 历史开关默认关闭；开启时可列出、切换并继续同 scope 历史，替换 App、Agent、tenant 或 external user 后均不能读取原历史。
- 历史关闭时不请求也不展示列表，刷新仍能通过 `sessionStorage` 中的精确 Session ID 恢复当前对话和草稿。
- 管理员可把 App 改为免登录公开 Widget；member 被拒绝。公开 App 的 Agent 数量、history、Origin 和工具配置不满足约束时保存失败。
- 相同 App 与 visitor key 轮换 credential 后仍恢复同一当前 Session，首条消息前数据库中没有 Hub Session，公开 Widget 不展示或读取历史列表。
- Agent 工具选择进入执行配置 fingerprint；App 只能进一步收紧工具。公开 Widget 即使 Agent 配置了写入、shell、Integration 或 MCP，也只能得到只读文件工具。

## 测试计划

- Rust：覆盖 secret hash、两种 grant、scope 子集、userinfo、应用/用户 principal、权限即时失效、origin 隔离、消息顺序和 tool result 继续。
- TypeScript：前端构建通过。
- 浏览器：覆盖 Integration App 新建/编辑/轮换、Agent 关联、OAuth grants、Session/message/SSE、Widget 地址和权限收回。
- Widget 浏览器：覆盖 15 分钟 credential 续期、两轮复用 Session ID、history on/off、刷新恢复、切换时旧 Run 后台继续、OAuth/JWT retry 和同 credential 双提交锁。
- 公开 Widget：覆盖 visitor credential 轮换、首条消息延迟建 Session、精确 Session 刷新恢复、visitor 隔离、无历史和 desktop/390px。
- 工具策略：覆盖 Agent/App 表单、App 子集校验、Run claim 有效交集，以及公开 Widget 对写入、shell、Integration、网络和 MCP 的强制收紧。
