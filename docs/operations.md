# Agent Hub Operations

本文说明 Native Codex Session Runtime 的部署配置、持久化边界和故障处理。协议与状态机的权威定义仍在 `docs/session-runtime-spec.md` 和 ADR-0001 至 ADR-0027。

## 生产 Compose

根目录 `compose.yml` 是默认生产编排。它启动 PostgreSQL、私有 MinIO、Hub 和无状态 Model Gateway；Hub backend 镜像同时包含并直接托管 React/Vite 静态资源，不需要独立 frontend 或 Nginx 容器。生产配置不启用 Mock OIDC、开发用户、开发 Model Connection、fake provider 或 fake Codex。先以 `.env.example` 为清单配置 `.env` 并执行 `chmod 600 .env`；关键值为空时 Compose 会在创建容器前拒绝启动。`.env` 已同时被 Git 和 Docker build context 排除。

至少需要设置：

- `POSTGRES_PASSWORD` 和与其一致的 `DATABASE_URL`。建议密码使用 URL-safe 随机值，避免连接串编码歧义。
- `HUB_MODEL_SECRET_KEY`，使用 `openssl rand -base64 32` 生成并纳入独立备份。
- `HUB_MODEL_GATEWAY_AUTH_TOKEN`，使用 `openssl rand -hex 32` 生成，只供 Hub 和 Model Gateway 之间鉴权。
- `EMBED_JWT_SECRET`，使用部署专属随机值。
- `HUB_BUNDLE_S3_ACCESS_KEY_ID` 和 `HUB_BUNDLE_S3_SECRET_ACCESS_KEY`，只用于 Compose 内部、不对宿主机发布端口的 MinIO。

默认启动生产 Hub：

```bash
docker compose up -d --build
docker compose ps
```

Hub 默认只发布到 `127.0.0.1:8080`，应由宿主机反向代理终止 TLS。只有明确由外部负载均衡器保护时才修改 `FRONTEND_BIND_ADDRESS`；生产环境保持 `SESSION_COOKIE_SECURE=true`。

Model Gateway 不发布宿主机端口，只连接 `model-network`；Runtime 只连接 `hub-network`，不是 `model-network` 成员，不能访问 Compose 中的 gateway/provider 服务地址。Backend 同时连接两个网络，并等待 gateway `/readyz` 健康后启动。Gateway 不接收数据库、S3、OAuth 或 provider 的持久凭据，provider key 只在单次 Hub 请求中传入。

生产 Runtime 是可选 profile，因为 Runtime 可以部署在其他节点。若要在同一台机器运行，先把可执行的真实 Codex CLI 放到 `CODEX_BIN_PATH`，在管理台创建一次性 Enrollment Token，并设置 `RUNTIME_ENROLLMENT_TOKEN`、实际 `RUNTIME_CODEX_VERSION` 和稳定的 `RUNTIME_HOSTNAME`，然后启动：

```bash
docker compose --profile runtime up -d --build
```

该 Codex CLI 是 Runtime 首次注册和兼容性检查所需的 bootstrap 版本；后续精确版本仍由 Hub 下载、校验并分发。`runtime-data` 已包含 Runtime credential、Workspace 和 Hub 分发的 Codex 版本，不得当作无状态缓存删除。

## 开发 Compose

`compose.dev.yml` 保留 Mock OIDC、开发种子、fake model provider 和 fake Codex，只用于本地开发及自动化测试。它同样由 backend 直接提供构建后的前端资源：

```bash
docker compose -p agent-hub-dev -f compose.dev.yml up -d --build
```

前端默认发布到宿主机 `15173`。端口冲突时必须在启动和后续 Playwright 命令中使用同一个 Compose project：

```bash
FRONTEND_PORT=15183 docker compose -p agent-hub-dev -f compose.dev.yml up -d --build
E2E_COMPOSE_PROJECT=agent-hub-dev npm --prefix frontend run test:e2e
```

两个 Compose 文件使用相同的四类持久卷，但不同 project name 会创建彼此隔离的实际卷：

| Volume | 容器路径 | 内容 |
| --- | --- | --- |
| `postgres-data` | `/var/lib/postgresql/data` | Hub 数据库 |
| `hub-data` | `/var/lib/agent-hub` | Hub 已校验的 Codex CLI 版本文件 |
| `runtime-data` | `/var/lib/agent-hub-runtime` | Runtime credential、在线 Session 目录、已下载的 Codex CLI 和暂存文件 |
| `bundle-store-data` | `/data` | Compose 内置 MinIO 中的 Session Bundle 对象 |

不要在 Runtime 尚有 Session 时删除 `runtime-data`。其中可能有尚未成功写入 Session Bundle 的最新 Workspace 状态。

## Runtime 注册和凭据

Administrator 在 Runtime 管理页点击“新增运行节点”打开抽屉。抽屉列出部署、设置环境变量和启动的步骤，并在用户明确创建后一次性展示 Runtime Enrollment Token。Token 有效期为 30 分钟，只能成功消费一次；Hub 只保存其哈希。

Runtime 主页在抽屉外单独显示仍未使用、未撤销且未过期的 Enrollment Tokens。已消费、已撤销或已过期记录不进入该列表。

Runtime 首次启动时读取 `RUNTIME_ENROLLMENT_TOKEN`，注册成功后把长期 Runtime Credential 原子写入 `RUNTIME_CREDENTIAL_FILE`。Unix 上该文件必须是 `0600`，否则 Runtime 拒绝启动。Compose 默认值如下：

```text
RUNTIME_CREDENTIAL_FILE=/var/lib/agent-hub-runtime/runtime-credential.json
RUNTIME_WORK_ROOT=/var/lib/agent-hub-runtime
RUNTIME_SESSION_IDLE_TIMEOUT_SECS=900
RUNTIME_MAX_ONLINE_SESSIONS=4
```

凭据文件存在时，Runtime 重启直接使用它，不再次消费 Enrollment Token。管理员发起 credential rotation 后，Runtime 先持久化新凭据、用新凭据完成交接，再废止旧凭据；不要手工编辑该文件。

Runtime 被普通或强制删除后，原 Runtime identity 和 credential 永久失效。需要重新接入该机器时，先保留或处置本地 Session 数据，再删除旧 credential 文件，并使用新 Enrollment Token 注册为新的 Runtime。

## Runtime 本地数据和 Session Bundle

每个在线 Session 位于 `RUNTIME_WORK_ROOT/sessions/<session-id>/`，包含独立的 `workspace/`、生成的 `codex/`、本地 `supervisor/` 元数据和 `staging/`。Runtime 还在同一持久根目录保存按精确版本安装的 Codex CLI。不同 Session 不共享 Workspace 或 `CODEX_HOME`。

Session 空闲 15 分钟后，Runtime 默认停止其 app-server 并生成一个 `tar.zst` Session Bundle。Bundle 只包含：

| 路径 | 用途 |
| --- | --- |
| `workspace/` | Session 的完整工作区，包括隐藏文件和 `.git` |
| `codex-thread/` | 恢复同一 native Codex Thread 所需的最小 transcript 和 index 文件 |
| `manifest.json` | Session/Thread 标识、Bundle generation、Hub history checkpoint、生成时 Codex 版本，以及内容大小和校验声明 |

Bundle 不包含 Runtime credential、模型密钥、OAuth secret、Agent/Skill/MCP 配置、日志、缓存、Codex CLI 或其他 Session。Agent/Skill/MCP 文件由 Hub 在下一 Turn 前按当前配置重新生成。

物理删除 Hub Skill 会请求相关在线 Session 刷新派生配置。空闲 Session 立即处理，活动 Turn 结束或被停止后处理；这只修改 `codex/` 中 Hub-owned 文件，不修改 `workspace/`、Bundle 或 native transcript。

Runtime 创建压缩包并计算压缩文件的 SHA-256。Hub 只校验身份、Session ownership generation 和声明的压缩大小，然后流式转发，不解包、不扫描，也不重新计算 checksum。恢复时仍由 Runtime 校验 checksum 后安全解包。

`HUB_SESSION_BUNDLE_MAX_BYTES` 控制单个压缩 Bundle 上限，默认 `10737418240` 字节（10 GiB）。反向代理和对象存储的请求体上限、超时及磁盘配额也必须允许该值，否则 checkpoint 会失败。

## 模型密钥和代理

Hub 启动必须提供 `HUB_MODEL_SECRET_KEY`，用于加密数据库中的 Model Connection API Key。该值是部署 secret，不得写入镜像、源码、日志、浏览器响应或 Runtime 配置；Compose 中的固定开发值只能用于本地测试。第一版没有 key rotation，修改或丢失该值会导致既有模型密钥无法解密。

Runtime 和 per-Session `CODEX_HOME` 只包含指向 loopback/Hub proxy 的 provider 配置，不包含真实 provider URL credential。`HUB_MODEL_PROXY_UPSTREAM_URL`、`HUB_MODEL_PROXY_API_KEY` 和全部 `RUNTIME_DIRECT_MODEL_*` 已废弃且不迁移；管理员必须在管理台创建 Global/Personal Model Connection。连接的服务根地址不带 `/v1`。Hub 完成 Run 授权、密钥解密和账本归属后，把单次请求交给内部 Model Gateway；Gateway 根据连接协议调用 provider，并把规范化 Responses JSON/SSE 返回 Hub。

`HUB_MODEL_GATEWAY_URL` 和 `HUB_MODEL_GATEWAY_AUTH_TOKEN` 是 Hub 启动必需配置。Compose 固定 URL 为 `http://model-gateway:8090`，共享令牌由 `.env` 注入 Hub 和 Gateway。`MODEL_GATEWAY_UPSTREAM_TIMEOUT` 与 `MODEL_GATEWAY_STREAM_IDLE_TIMEOUT` 使用 Go duration 语法；默认分别为 `300s` 和 `120s`。Gateway 不保存 provider key、请求、响应、用量或错误历史，重启不需要持久卷。

Gateway 提供内部 `GET /healthz` 和 `GET /readyz`，两者只表示进程已可接收请求，不主动探测任意动态 provider。可用 `docker compose ps` 检查 Compose 健康状态，用 `docker compose logs model-gateway` 查看仅含 request ID、协议和进程生命周期错误的日志；日志不应出现 endpoint credential、prompt 或 output。Gateway 不重试模型调用，故故障恢复后由上层明确发起新请求，不会在 Gateway 内重复计费。

Personal Model Connection 地址按 ADR-0024 不做公网或内网限制。允许普通用户使 Hub 访问其网络可达的 HTTP/HTTPS 地址是明确接受的部署风险；需要隔离时必须在 Hub 所在网络或外部 egress 层实施，而不是依赖本应用过滤。HTTPS 始终使用标准证书和 hostname 校验；自签名 endpoint 应使用受控 HTTP 或在部署信任库中正确安装 CA，应用不提供跳过 TLS 校验的开关。Global 连接的 Administrator 与 Personal 连接的 owner 还必须信任 provider 接收 API Key 并生成响应；OpenAI 字节透明 body 不做 credential 内容扫描，Hub/Gateway 只保证自身生成的数据和转发 header 不泄露 key。

## S3-compatible 对象存储

对象存储不是 Hub 启动的强制依赖。未设置 `HUB_BUNDLE_S3_ENDPOINT` 时，在线 Session 仍可运行，但 Bundle 上传不可用；Runtime 会保留本地最新状态并重试，Runtime drain 和跨节点恢复可能因此无法完成。生产环境应配置 S3-compatible 存储：

```text
HUB_BUNDLE_S3_ENDPOINT=https://s3.example.com
HUB_BUNDLE_S3_BUCKET=agent-hub-sessions
HUB_BUNDLE_S3_REGION=us-east-1
HUB_BUNDLE_S3_ACCESS_KEY_ID=...
HUB_BUNDLE_S3_SECRET_ACCESS_KEY=...
HUB_BUNDLE_S3_SESSION_TOKEN=
HUB_BUNDLE_S3_SERVER_SIDE_ENCRYPTION=AES256
HUB_BUNDLE_S3_KMS_KEY_ID=
HUB_BUNDLE_S3_ALLOW_HTTP=false
```

Endpoint 只允许 HTTP/HTTPS，且不能包含 query 或 fragment。HTTPS 是默认要求；仅受控内网的开发存储需要 HTTP 时，显式设置 `HUB_BUNDLE_S3_ALLOW_HTTP=true`。Server-side encryption 可留空，也可设为 `AES256` 或 `aws:kms`；只有 `aws:kms` 可同时设置 KMS key ID。

S3 credential、bucket 和对象 URL 只存在于 Hub。Runtime 的上传和下载都只连接 Hub，不获得 S3 credential、预签名 URL，也不直接访问对象存储。

## 精确 Codex CLI 版本发布

Hub 默认从 `https://api.github.com/repos/openai/codex` 获取官方 release，并把已验证 artifact 保存到 `HUB_CODEX_ARTIFACT_ROOT`（Compose 中为持久化的 `/var/lib/agent-hub/codex-artifacts`）。测试镜像源只有在显式设置 `HUB_CODEX_GITHUB_ALLOW_HTTP=true` 后才能使用 HTTP。

发布流程：

1. 在 Administration 页面输入明确版本号，例如 `0.104.0`；不接受 `latest`。
2. Hub 按所有已注册 Runtime 的 OS/architecture 下载官方 release artifact 并验证发布的 SHA-256。
3. Runtime 只从 Hub 下载候选文件，再次校验并运行有界的基础兼容性检查，然后报告 readiness。
4. 所有要求的平台 ready 后，管理员 Promote 该 Target Codex Version 为全局 Active Codex Version。
5. 正在运行的 Turn 继续使用旧进程直到结束。Session 随后用旧版本完成 checkpoint，下一 Turn 直接使用新 Active Codex Version；不做跨版本恢复冒烟测试，也不静默回退旧版本。

恢复始终使用当前 Active Codex Version。若它无法继续原 native Thread，原 Bundle 保持不变，Session 进入 `recovery_failed`，不会创建替代 Thread。

## Runtime drain 和删除

计划维护或下线 Runtime 时：

1. 在 Runtime 管理页核对 hostname 和受影响 Session，执行 Drain。
2. Draining Runtime 不再领取新 Session。活动 Turn 先正常结束；随后每个 Session checkpoint 并释放 ownership。
3. 等受影响 Session 清空后执行普通 Delete。仍持有 Session 时普通删除会被拒绝。

Cancel Drain 只让该 Runtime 恢复领取能力；已经释放并被其他 Runtime 接管的 Session 不会自动迁回。

Force Delete 只用于确认节点永久丢失或无法完成 drain 的情况。它立即撤销 Runtime credential 并使旧 ownership generation 失效。已有 current Bundle 的 Session 可由其他 Runtime 恢复；没有覆盖最新状态的 Bundle 时，确认列表中的 Session 会进入 `recovery_failed`。该操作不可通过旧 credential 撤回。

## `recovery_failed` 处理

`recovery_failed` 表示 Hub 最新历史无法匹配一个完整可恢复的 Workspace 和 native Codex Thread，常见原因是永久丢失带有未 checkpoint 数据的 Runtime，或 Active Codex Version 无法恢复原 Thread。

- 保留 Session 历史和最后一个不可变 Bundle，禁止继续发送消息。
- 不回滚到旧 Workspace，不从 Hub 消息重建新 Thread，也不自动切回旧 Codex 版本。
- 先保留对象存储和数据库记录，再检查 Runtime、Hub 日志及 Active Codex Version。不要用 Force Delete 作为普通重试手段。
- 当前产品没有把 `recovery_failed` Session 原地改回可执行状态的管理操作；它只能作为只读历史保留，或随其 Hub User 被不可逆删除。
