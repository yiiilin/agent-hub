# Auth 功能链 Spec

## 身份模型

1. Hub User 由不可变 UUID 标识；`email` 必填、按规范化值全局唯一，并作为唯一 Hub 登录名与可信邮箱绑定键。
2. Hub User 不再包含 `username` 或 `email_verified`。`display_name` 是可重复、可修改的展示资料，不参与认证或授权。
3. Hub 不保存 LDAP 永久用户 ID 或历史邮箱别名。管理员修改邮箱后，LDAP 再返回旧邮箱时可以创建新的 Hub User，这是已接受行为。
4. External Identity 仍由 External Platform、Tenant 和 external user ID 唯一定位；其可选 external username 只是外部资料，不是 Hub 登录名。
5. 认证 Integration App 的 Client Access Credential 和 `client_credentials` External Session 首次关联用户时必须提供合法可信邮箱；`authorization_code` 使用 Token 已绑定的 Hub User。匿名应用不创建 Hub User，也不提供历史发现。

## Hub 登录方式

1. Hub 控制面只支持 Local Password Login 和一个全局 LDAP Directory；Mock OIDC 完全删除。Integration App 的 OAuth、External Platform 和 Authentication Channel 保留，但不是 Hub 登录方式。
2. 全新空库默认开启 Password Registration 与普通 Local Password Login，LDAP 默认关闭。第一位成功创建的用户原子成为 `super_admin`，并自动关闭 Password Registration。
3. `admin` 或 `super_admin` 可重新开启 Password Registration，但只有普通 Local Password Login 已开启时才允许。管理台必须持续提示无邮箱验证会造成 LDAP 邮箱被预注册的风险。
4. Password Registration 接受邮箱、密码和可选 `display_name`；缺少展示名时使用邮箱 `@` 前部分。本项目不实现邮箱验证，注册成功后邮箱立即生效。
5. 禁止同时关闭普通 Local Password Login 和 LDAP Login。停用任一登录方式前，至少一个有效 `super_admin` 必须已设置本地密码。
6. 普通 Local Password Login 关闭后，`member` 与 `admin` 不能使用本地密码；已设置密码的 `super_admin` 仍可通过 `/login?method=password` 的隐藏紧急入口登录。常规登录页不展示密码按钮。
7. LDAP 关闭或配置改变不注销已有 browser Session；目录账号被停用也不会主动撤销现有 Session。现有 Session 继续到退出、管理员清除或七天到期，下一次 LDAP 登录由目录拒绝。

## LDAP 配置

1. 全平台只有一个数据库持久化 LDAP 配置。未配置时没有有效记录；首次保存必须完整。停用只关闭 LDAP Login，保留配置以便重新启用。
2. 配置字段为一个 LDAP URL、连接模式、Base DN、Bind 身份模板、用户查询模板、邮箱属性、展示名属性、允许明文开关和跳过 TLS 验证开关。AD 默认值：
   - Bind 身份模板：`{email}`
   - 查询模板：`(userPrincipalName={email})`
   - 邮箱属性：`mail`
   - 展示名属性：`displayName`
3. 连接模式是 `ldaps`、`starttls` 或 `plain`。`ldaps` 只接受 `ldaps://`；其余模式只接受 `ldap://`。URL 不得内嵌用户名或密码。
4. `plain` 必须显式允许不安全连接并持续展示警告。跳过证书验证默认关闭，只适用于 TLS 模式，并持续展示警告。V1 不支持自定义 CA。
5. 只支持一个服务器 URL，不实现应用内故障切换；高可用由 DNS、VIP 或外部负载均衡提供。
6. LDAP Login 将用户提交的完整邮箱按 Bind 身份模板替换后与密码直接 Bind，不需要或保存只读服务账号。模板必须恰好包含一个 `{email}`；邮箱按 LDAP DN 属性值转义。AD/UPN 通常使用 `{email}`，OpenLDAP 固定 DN 可使用 `uid={email},ou=people,dc=example,dc=test`。连接超时五秒，完整 Bind 与查询总超时十秒，不自动重试。
7. Bind 成功后，在 Base DN 下执行固定 `Subtree` 查询。查询模板必须恰好包含安全转义后的 `{email}`；结果必须恰好一条，否则登录失败。
8. 查询返回的邮箱属性必须是合法邮箱，并作为 Hub User 的权威邮箱；它可以不同于用户输入的 Bind 邮箱。展示名缺失时首次建号使用邮箱本地部分，后续登录只有非空目录值才覆盖当前 `display_name`。
9. LDAP 相同邮箱自动关联已有 Hub User，并保留其角色、本地密码、API Keys 和数据；不存在时创建 `member`，空库首个用户创建为 `super_admin`。正在删除的同邮箱用户阻止登录，直到删除完成。
10. 不做 LDAP group 到 Hub role 的映射，不保存 LDAP 用户 ID，也不按每个 API 请求重新验证目录状态。

## LDAP 管理测试

1. `admin` 与 `super_admin` 在“管理 -> 认证”维护配置。测试操作使用当前未保存的表单草稿，加一次性邮箱和密码执行真实 Bind、Subtree 查询和字段映射；成功或失败都不会自动保存配置。
2. 成功只返回目录邮箱、展示名和耗时。失败返回管理员可用的失败阶段与清理后诊断，不返回用户 DN、原始目录属性或密码。
3. 测试凭证和结果不入库、不写日志。测试失败计入同邮箱五分钟三次的 LDAP 失败限制，但不计入普通登录 IP 总尝试限制。

## 登录限流与日志

1. Local Password Login 与 LDAP Login 共用数据库持久化限流：同一规范化邮箱五分钟最多三次失败，同一来源 IP 五分钟最多二十次登录尝试；服务在验证密码或 LDAP 凭据前原子占用邮箱失败额度，成功后清除，确保并发请求也不能越过第三次真实凭据校验。超限返回 `429` 和可用的 `Retry-After`。
2. 默认信任 `Forwarded` / `X-Forwarded-For` 提供的客户端 IP；这允许直连客户端伪造 IP 并绕过 IP 限流。配置 `TRUSTED_PROXY_CIDRS` 后，仅当 TCP 来源属于指定网段才信任转发头，否则使用 TCP 来源 IP。
3. 普通 LDAP 登录把用户不存在、密码错误、字段错误和结果不唯一统一返回“邮箱或密码错误”；连接、超时或配置故障返回“LDAP 服务暂不可用”。
4. 不新增完整登录审计表。服务日志只记录请求 ID、耗时、LDAP 阶段和错误类别，不记录邮箱、密码、Bind 身份、用户 DN 或目录属性；数据库只保留限流状态。

## 用户管理

1. 用户可修改自己的 `display_name`。管理员可在权限边界内查看用户、修改邮箱和展示名、设置本地密码、修改角色或永久删除账户。
2. `admin` 创建账户时只能创建 `member`；`super_admin` 可创建 `member`、`admin` 或 `super_admin`。邮箱必填，展示名和密码可选；无密码账户只能通过 LDAP 登录，直到管理员设置密码。
3. 管理员改邮箱或密码后删除目标用户全部 browser Sessions，保留 API Keys、External Identities、Integration App/Application Token 和所有账户数据。邮箱冲突返回 `409`。
4. `admin` 不得读取或修改 `super_admin`；只有 `super_admin` 可授予或撤销 `admin`/`super_admin`，且不得移除最后一个 Super Administrator。
5. 永久删除使用目标邮箱确认。删除开始后立即阻止认证；所有数据清理完成前邮箱不可复用，完成后才可重新注册或由 LDAP 创建。

## 既有凭据边界

1. Browser Session、API Key 和 Embed JWT 继续解析为明确 `AuthPrincipal`；Application Token 使用独立 principal，不进入普通用户控制面。
2. API Key 创建时必须指定 30/90/180/365 天、自定义未来日期或永久；未指定时默认 90 天。续期只延长有效期或改为永久，不轮换或重新显示 Token。
3. API Key 只有物理删除，没有撤销与恢复；权限与所属 Hub User browser Session 相同，不增加细粒度 scope。改邮箱或改密码不删除 API Key。
4. Embed JWT 只用于换取短期 Widget/Client credential，不用于 Hub 登录。Client credential、续期、草稿、提交锁和 Session 隔离遵循 `docs/integration-spec.md`。

## 公共接口

- `POST /api/auth/register`：Password Registration；首个用户成功后原子关闭注册。
- `POST /api/auth/login`：Local Password Login，包括隐藏入口中的 Super Administrator 紧急登录。
- `POST /api/auth/ldap/login`：LDAP Login。
- `GET /api/auth/providers`：返回注册、普通密码登录和 LDAP 登录的当前可用状态，不返回 Mock OIDC 或邮箱验证字段。
- `GET /api/auth/me`、`POST /api/auth/logout`：browser Session 生命周期。
- `PATCH /api/users/me`：只修改当前用户 `display_name`。
- `GET|PATCH /api/admin/auth-policy`：认证策略与跨开关约束。
- `GET|PUT /api/admin/ldap-config`、`POST /api/admin/ldap-config/test`：完整 LDAP 配置和草稿测试。
- `POST /api/admin/users`、`PATCH /api/admin/users/{user_id}`：管理员建号与邮箱/展示名维护；既有 password、role 和 erasure 子资源保留。

## 非目标

- SMTP、邮件发送、验证 Token 或任何邮箱确认流程。
- Mock OIDC Hub 登录、多 LDAP 配置、LDAP server failover、服务账号、group-role 映射、永久 LDAP ID、历史邮箱别名或自定义 CA。
- 每请求 LDAP 复验、LDAP 停用时主动注销 Session、完整登录审计表或测试成功后自动保存配置。
- 旧 `username`、`email_verified`、无邮箱 Hub User 或旧数据库的兼容迁移。

## 验收与测试

- Rust/SQL：最终初始 schema、邮箱唯一性、并发首用户、注册自动关闭、策略锁定保护、Super Administrator 紧急登录、管理员权限、Session 注销、限流持久化和 secret/log redaction。
- LDAP：真实 OpenLDAP 覆盖 Plain、StartTLS、LDAPS、自签名证书拒绝与显式跳过、Bind、Subtree、过滤器转义、0/1/多结果、字段映射、超时分类、无重试和配置草稿测试。
- Integration：认证 Client Access 与 `client_credentials` Session 缺少邮箱时失败，相同邮箱稳定绑定；匿名流程不受影响。
- Browser：普通/LDAP/隐藏紧急登录、注册策略、LDAP 管理与测试、用户自助展示名、管理员建号/改邮箱/改展示名/改密/删除，验证 desktop、390px、中文/英文、console 和 network。
- 门禁：相关 Rust 与 SQLx 测试、前端构建、真实 Compose LDAP API/Browser 场景和一次 workspace build。

## Embed JWT Claim

最小 claim：`iss`、`aud`、`exp`、`iat`、`jti`、`sub`、`owner_id`、`agent_id`。Hub 只在 owner 对未删除 Agent 仍有调用权限时签发 opaque Client credential。
