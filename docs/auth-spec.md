# Auth 功能链 Spec

## 范围

1. 保留 password + browser session cookie 登录、可配置的密码注册/登录/邮箱验证策略和受信 Authentication Channel 自动绑定。
2. session、API Key 和 Embed JWT 继续解析为明确 `AuthPrincipal`；Application Token 使用独立 principal，不进入普通用户控制面。
3. API Key 创建时必须指定有效期：30/90/180/365 天、自定义未来日期或永久；未显式指定时默认 90 天。
4. API Key 续期只把到期时间延长到更晚日期或改为永久，不更换、不重新显示 token。永久 key 不能再续期。
5. API Key 只有物理删除，没有撤销与恢复；到期或删除后立即无法认证。
6. API Key 权限与所属 Hub User browser session 相同，不增加细粒度 scope。
7. Embed JWT 只用于换取短期 Widget session，不用于普通用户登录。
8. Administrator 可查看、改密和删除非 `super_admin` 用户；Super Administrator 可处理任意非自身保护约束的用户。`admin` 不得读取或修改 `super_admin` 账户、凭据或个人资源。
9. 只有 Super Administrator 可授予或撤销 `admin`/`super_admin` 角色，且不得移除最后一个 Super Administrator。Global resources 不属于创建者个人，`admin` 与 `super_admin` 都可管理。
10. 管理员改密后删除目标用户的全部 browser sessions，保留 API Keys、External Identities 和 Integration App/Application Token 记录。
11. OAuth profile scopes 和 `userinfo` 遵守 `docs/integration-spec.md`，不与 Hub 登录 provider 混用。
12. Integration App 的服务端使用 `client_id`/`client_secret` 通过 HTTP Basic 调用 `POST /api/widget/access`，为一个 Agent、External Tenant 和 External Identity 签发 15 分钟 `ahw_` Widget Access Credential；client secret 不进入浏览器、Widget URL、日志或前端存储。
13. Widget 使用当前 `ahw_` 调用 `POST /api/widget/session/renew` 原位轮换凭证。普通自动续期不需要应用 secret；只有更新受信外部用户资料时才同时要求当前 Widget credential 和该 Integration App 的 HTTP Basic 凭据。
14. Widget credential 只保存在当前标签页的 `sessionStorage`。刷新可恢复当前草稿和精确的 Integration/Hub Session ID；关闭标签页后不依赖浏览器持久化重新发现关闭了历史功能的 Session。
15. Embed JWT exchange 失败或重试不得先清空现有 Widget credential、草稿或在途提交锁；成功选择不同 credential 才切换逻辑会话，同 token 选择是 no-op。

## 非目标

- 不实现 API Key 细粒度 scope、自动轮换或可恢复撤销。
- 不把 Embed JWT 或 Application Token 用作普通 Hub 登录凭据。
- 不因管理员改密删除 API Key 或外部应用授权。

## 验收标准

- `/api/auth/me` 同时支持有效 session cookie 和未到期 API Key。
- 用户显式退出登录时，管理台清除当前浏览器中归属于该 Hub User 的全部 Conversation Draft；刷新、关闭浏览器和登录会话自然过期不主动清除 Draft。
- API Key 明文只在创建响应中返回一次，管理台单行显示并在 hover/focus 时显示复制按钮。
- `ApiKeyDto` 包含 nullable `expires_at`而不包含 `revoked_at`；过期 key 返回 401。
- 续期保留 id、name、prefix、token hash、created/last-used 记录，并拒绝相同或更早日期。
- 续期和删除仅允许 owner 通过 browser session 或另一把有效 API Key 执行；当前 key 不能操作自身记录。
- Administration 对 `admin` 和 `super_admin` 可见，并按 protected Super Administrator 边界提供用户、认证、外部平台、Runtime 和 Model Connection 管理。改密后旧 browser session 失效，API Key 仍然有效。
- `ahw_` 自动续期只轮换 token，不改变其稳定 Widget session 记录，不清空草稿、不解除在途消息的单提交锁，也不让迟到响应进入另一个逻辑会话。

## 测试计划

- Rust：覆盖 API Key 默认/预设/自定义/永久有效期、过期认证、同 token 续期、自操作拒绝、owner 隔离、物理删除和管理员改密的 session 注销边界。
- TypeScript：前端构建通过。
- 浏览器：覆盖 API Key 创建/复制/续期/删除/到期显示，以及用户详情、改密和删除。
- Widget：覆盖应用后端签发、普通续期、受信资料更新的双重认证、exchange 失败保留状态、同 credential 双提交锁和 `sessionStorage` 刷新恢复。

## Embed JWT Claim

最小 claim：`iss`、`aud`、`exp`、`iat`、`jti`、`sub`、`owner_id`、`agent_id`。Hub 只在 owner 对未删除 Agent 仍有调用权限时签发 opaque Widget session。
