# Agent Hub Operations

本文说明 Native Pi Session Runtime 的部署配置、持久化边界和故障处理。协议与状态机的权威定义仍在 `docs/session-runtime-spec.md`、`docs/pi-driver-spec.md` 和 ADR-0001 至 ADR-0027。

## Pi Runtime artifact

Runtime 镜像内置 Pi `v0.81.1`，源码固定为 git submodule `third_party/pi` 的
commit `20be4b18d4c57487f8993d2762bace129f0cf7c6`。构建期使用 Node/npm 和固定
Bun `1.3.14`；最终 Runtime 镜像不安装 Node、npm 或 Bun。Linux x64 必须使用
`bun-linux-x64-baseline`，不能使用依赖 AVX/AVX2 的通用 artifact。

从已初始化 submodule 的干净 checkout 重建：

```bash
git submodule update --init --recursive third_party/pi
./scripts/build-pi-standalone.sh
target/pi-runtime/linux-x64/pi --version
```

输出必须是 `0.81.1`。脚本校验 submodule commit、Bun archive SHA-256、
`third_party/pi-patches/0001-add-rpc-reload-models.patch` SHA-256、以及
`third_party/pi-model-data/v0.81.1/` 的完整 tree SHA-256，然后在 `/tmp` 的
一次性源码导出中应用补丁并构建 release directory。`--skip-install` 只用于本机
已存在与 lockfile 匹配的 `third_party/pi/node_modules` 时复用依赖；不能作为干净
构建证明。构建还会检查编译后的 RPC mode 同时包含 `reload_resources` 和
`reload_models`，防止生成无法在线重载 Skill 或模型配置的 artifact。

model-data snapshot 的 freshness 定义为：目录版本与 Pi pin 相同，tree hash 与
构建脚本常量一致，并且该 pin 的 offline build 通过。升级 Pi、修改 provider
适配或上游 model catalog 变化时，必须重新生成 snapshot、更新 tree hash、重建
artifact 并执行真实 standalone RPC smoke；不能仅修改版本字符串。

运行时只执行镜像内的 Pi binary，不能让 Runtime 下载 GitHub release，也不能
把 provider key 写入 Pi 配置。模型请求始终经 Runtime loopback -> Hub model
gateway；Bundle 只保存 Pi recovery JSONL 和 Workspace。Hub 不提供执行引擎
rollout API，Pi 随完整 Runtime 镜像发布。

Session 配置原子物化成功后，Runtime 对空闲 Pi 调用原生 `reload_resources`，使其
重新发现 AGENTS.md 和 Skills；活动 Turn 延迟到终态后执行。这个操作不重启 Pi
进程或创建新的 Native Session。只有最终工具集合变化时，Runtime 才在下一 Turn
前替换空闲进程，并从原 JSONL 恢复同一个 Native Session。

## 生产 Compose

根目录 `compose.yml` 是默认生产编排。它启动 PostgreSQL、私有 MinIO、Hub 和无状态 Model Gateway；Hub backend 镜像同时包含并直接托管 React/Vite 静态资源，不需要独立 frontend 或 Nginx 容器。Hub 不包含 Mock OIDC，生产配置也不启用开发用户、开发 Model Connection、fake provider 或 fake Pi RPC。先以 `.env.example` 为清单配置 `.env` 并执行 `chmod 600 .env`；关键值为空时 Compose 会在创建容器前拒绝启动。`.env` 已同时被 Git 和 Docker build context 排除。

正式版本只能从 `main` 分支上的版本标签发布，在 GHCR 发布三张同版本镜像。Release
workflow 会拒绝不属于 `origin/main` 历史的标签：

| 服务 | 默认镜像 |
| --- | --- |
| Hub 与管理台 | `ghcr.io/yiiilin/agent-hub:0.1.0` |
| Runtime 与 Pi standalone | `ghcr.io/yiiilin/agent-hub-runtime:0.1.0` |
| Model Gateway | `ghcr.io/yiiilin/agent-hub-gateway:0.1.0` |

Release workflow 先对完整 Git 历史执行凭据扫描，再在 GitHub runner 本地构建候选镜像。
候选镜像的 config、每个最终 image layer 和二进制文件的可打印字符串均通过 Gitleaks
脱敏扫描后，workflow 才登录 GHCR 并推送同一个本地镜像。扫描报告、Buildx cache 和
build record 均不上传；任何扫描发现都会在登录 registry 前终止该镜像任务。创建 release
tag 前仍需在本地对待推送历史执行同一份 `.gitleaks.toml`，因为远端 workflow 无法撤回
已经推送的 Git 对象。

标签推送未产生 workflow 运行时，不移动或覆盖现有标签；从 `main` 手动重试并明确指定
已有版本标签：

```bash
gh workflow run Release --ref main -f release_tag=v0.1.0
```

固定 Pi `0.81.1` 的 compiled binary 会被 Gitleaks `8.28.0` 的通用规则命中 6 个源码
表达式。`scripts/scan-image-secrets.sh` 不按规则或整个文件放行，而是同时固定命中数量与
排序后 `RuleID`/完整 `Match` 集合的 SHA-256；原始命中只存在于 runner 的 `0700` 临时
目录。Pi、Gitleaks 或编译输出发生变化时，基线不再匹配并阻断发布，必须人工复核全部
脱敏命中后才能更新该摘要。

`AGENT_HUB_IMAGE_REGISTRY` 和 `AGENT_HUB_IMAGE_TAG` 可统一覆盖 registry 与版本。生产升级时只修改一次版本值，三张镜像必须保持相同 Agent Hub 版本。若 Package 不是公开可读，先使用仅具备 `read:packages` 权限的部署凭证登录，且不要把凭证写入 `.env`、Compose 或 shell history：

```bash
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io --username YOUR_GITHUB_USER --password-stdin
```

至少需要设置：

- `POSTGRES_PASSWORD` 和与其一致的 `DATABASE_URL`。建议密码使用 URL-safe 随机值，避免连接串编码歧义。
- `HUB_MODEL_SECRET_KEY`，使用 `openssl rand -base64 32` 生成并纳入独立备份。
- `HUB_MODEL_GATEWAY_AUTH_TOKEN`，使用 `openssl rand -hex 32` 生成，只供 Hub 和 Model Gateway 之间鉴权。
- `EMBED_JWT_SECRET`，使用部署专属随机值。
- `HUB_BUNDLE_S3_ACCESS_KEY_ID` 和 `HUB_BUNDLE_S3_SECRET_ACCESS_KEY`，只用于 Compose 内部、不对宿主机发布端口的 MinIO。
- Compose 默认 `HUB_SKILL_PACKAGE_STORAGE=s3`，Skill Package 与 Session Bundle 共用上述私有 S3-compatible 服务和凭据，但使用独立对象前缀。需要改为 Hub 本地卷时显式设为 `local`。

默认启动生产 Hub：

```bash
docker compose pull
docker compose up -d
docker compose ps
```

Hub 默认只发布到 `127.0.0.1:8080`，应由宿主机反向代理终止 TLS。只有明确由外部负载均衡器保护时才修改 `FRONTEND_BIND_ADDRESS`；生产环境保持 `SESSION_COOKIE_SECURE=true`。

Model Gateway 不发布宿主机端口，只连接 `model-network`；Runtime 只连接 `hub-network`，不是 `model-network` 成员，不能访问 Compose 中的 gateway/provider 服务地址。Backend 同时连接两个网络，并等待 gateway `/readyz` 健康后启动。Gateway 不接收数据库、S3、OAuth 或 provider 的持久凭据，provider key 只在单次 Hub 请求中传入。

生产 Runtime 是可选 profile，因为 Runtime 可以部署在其他节点。若要在同一台机器运行，在管理台创建一次性 Enrollment Token，设置 `RUNTIME_ENROLLMENT_TOKEN` 和稳定的 `RUNTIME_HOSTNAME`，然后启动：

```bash
docker compose --profile runtime pull
docker compose --profile runtime up -d
```

Runtime 镜像固定执行 `/opt/agent-hub/pi/pi`，版本由镜像内
`RUNTIME_ENGINE_VERSION=0.81.1` 上报，Compose 不接受宿主机同名变量覆盖它；
`ENGINE_BIN` 固定指向镜像内二进制，也不从 Hub 下载执行 artifact。`runtime-data` 包含 Runtime credential、Workspace、在线
Pi Session state 和暂存文件，不得当作无状态缓存删除。

## 开发 Compose

`compose.dev.yml` 保留开发种子和 fake model provider，只用于本地开发及自动化测试；Runtime 仍执行镜像内真实 Pi standalone。确定性的 `deploy/fake-pi-rpc.sh` 只用于协议级测试。开发环境同样由 backend 直接提供构建后的前端资源：

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
| `hub-data` | `/var/lib/agent-hub` | Hub 持久数据；使用 local backend 时还包含 Skill Package 对象 |
| `runtime-data` | `/var/lib/agent-hub-runtime` | Runtime credential、在线 Session 目录、Skill 压缩缓存和暂存文件 |
| `bundle-store-data` | `/data` | Compose 内置 MinIO 中的 Session Bundle 与 Skill Package 对象 |

不要在 Runtime 尚有 Session 时删除 `runtime-data`。其中可能有尚未成功写入 Session Bundle 的最新 Workspace 状态。

## LDAP 登录与代理来源

LDAP 配置由 `admin` 或 `super_admin` 在“管理 -> 认证”写入数据库，不通过环境变量保存 Bind 账号或密码。Hub 只支持一个 LDAP URL，并使用配置的 Bind 身份模板将登录邮箱映射成目录接受的 Bind 名称；AD/UPN 通常使用 `{email}`，固定 DN 目录可使用 `uid={email},ou=people,dc=example,dc=test`。生产部署必须确保 Hub 网络能够访问该目录地址。LDAPS 与 StartTLS 默认执行证书和 hostname 校验；纯明文与跳过证书验证都需要在管理台显式开启，并持续展示风险警告。

开发与 QA 使用可选 `ldap` profile 启动真实 OpenLDAP；普通开发启动不承担该服务开销：

```bash
docker compose -p agent-hub-dev -f compose.dev.yml --profile ldap up -d --build
```

默认情况下 Hub 信任请求中的 `Forwarded` / `X-Forwarded-For` 作为登录限流来源，这意味着可直连 Hub 的客户端能够伪造 IP。生产环境应设置 `TRUSTED_PROXY_CIDRS` 为实际反向代理网段；配置后，只有 TCP 来源属于这些网段时才采用转发头，否则按 TCP 来源 IP 限流。

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

每个在线 Session 位于 `RUNTIME_WORK_ROOT/sessions/<session-id>/`，包含独立的 `workspace/`、作为隔离 Pi `HOME` 的 `engine-state/`、本地 `supervisor/` 元数据和 `staging/`。`engine-state/.pi/agent/` 是 Hub 可重建配置，`engine-state/sessions/` 是 Pi native JSONL，`engine-state/skill-exec/` 是该 Session 私有的 Skill Package 执行副本、catalog 和调用临时目录。不同 Session 不共享 Workspace、Pi HOME、解包目录或 JSONL。

`RUNTIME_ENGINE_TIMEOUT_SECS` 是单个 Pi Turn 的整轮硬截止，Compose 默认 `3600` 秒。计时不会因模型重试、流式输出或工具调用重置；到期后当前 Pi 子进程和 Run 会停止，Session 与 Workspace 保留，控制台和 Widget 会从持久 Run 事件显示超时错误。

Runtime 仅在 `RUNTIME_WORK_ROOT/skill-package-cache/` 共享按归档 SHA-256 命名的压缩缓存。该目录为 `0700`，缓存文件为 `0600`；每次命中仍校验大小和 checksum，损坏项原子重新下载。缓存不是权威数据，当前没有应用内 GC；部署需监控该目录容量，离线清理时应先停止 Runtime，且不要删除 Session 私有目录来代替清缓存。

Session 空闲 15 分钟后，Runtime 默认停止其 Pi RPC 进程并生成一个 `tar.zst` Session Bundle。Bundle 只包含：

| 路径 | 用途 |
| --- | --- |
| `workspace/` | Session 的完整工作区，包括隐藏文件和 `.git` |
| `native-session/sessions/<file>.jsonl` | 恢复同一 Native Session 的唯一 Pi JSONL |
| `manifest.json` | Hub/Pi Session 标识、Bundle generation、Hub history checkpoint、生成时 Pi 版本，以及内容大小和校验声明 |

Runtime 按 JSONL 第一行的 `type=session` 和 `id=<native_session_id>` 选择唯一恢复文件，不依赖文件名。恢复只接受 `native-session/`、`native-session/sessions/` 和一个直接子级 `.jsonl`，并在提交恢复目录前再次验证 header 与 manifest 匹配。Bundle 不包含 `.pi/agent`、`skill-exec/`、Skill Package 或 Runtime 压缩缓存、Runtime credential、模型密钥、OAuth secret、Agent/Skill/MCP 配置、settings、extensions、日志、Pi binary 或其他 Session。Agent/Skill/model binding 文件由 Hub 在下一 Turn 前按当前配置重新生成。

物理删除 Hub Skill 会请求相关在线 Session 刷新派生配置。空闲 Session 立即处理，活动 Turn 结束或被停止后处理；这只修改 `engine-state/` 中 Hub-owned 文件，不修改 `workspace/`、Bundle 或 native transcript。

Runtime 创建压缩包并计算压缩文件的 SHA-256。Hub 只校验身份、Session ownership generation 和声明的压缩大小，然后流式转发，不解包、不扫描，也不重新计算 checksum。恢复时仍由 Runtime 校验 checksum 后安全解包。

`HUB_SESSION_BUNDLE_MAX_BYTES` 控制单个压缩 Bundle 上限，默认 `10737418240` 字节（10 GiB）。反向代理和对象存储的请求体上限、超时及磁盘配额也必须允许该值，否则 checkpoint 会失败。

## 模型密钥和代理

Hub 启动必须提供 `HUB_MODEL_SECRET_KEY`，用于加密数据库中的 Model Connection API Key。该值是部署 secret，不得写入镜像、源码、日志、浏览器响应或 Runtime 配置；Compose 中的固定开发值只能用于本地测试。第一版没有 key rotation，修改或丢失该值会导致既有模型密钥无法解密。

Runtime 和 per-Session Pi HOME 只包含指向 loopback/Hub proxy 的 provider 配置，不包含真实 provider URL credential。`HUB_MODEL_PROXY_UPSTREAM_URL`、`HUB_MODEL_PROXY_API_KEY` 和全部 `RUNTIME_DIRECT_MODEL_*` 已废弃且不迁移；管理员必须在管理台创建 Global/Personal Model Connection。连接的服务根地址不带 `/v1`。Hub 完成 Run 授权、密钥解密和账本归属后，把单次请求交给内部 Model Gateway；Gateway 根据连接协议调用 provider，并把规范化 Responses JSON/SSE 返回 Hub。

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

## Skill Package 存储和运行

Skill Package 支持两种 Hub 存储 backend：

```text
HUB_SKILL_PACKAGE_STORAGE=s3
HUB_SKILL_PACKAGE_LOCAL_DIR=/var/lib/agent-hub/skill-packages
```

- `s3` 复用 `HUB_BUNDLE_S3_*` 配置；必须已设置 `HUB_BUNDLE_S3_ENDPOINT`。生产和开发 Compose 默认使用此模式。
- `local` 将对象原子写入 `HUB_SKILL_PACKAGE_LOCAL_DIR`，目录权限为 `0700`、文件为 `0600`。该目录必须位于持久卷并纳入备份；此模式不需要 S3 才能上传 Skill Package，也不改变 Session Bundle 自身是否可用。
- 未显式设置 backend 时，有 S3 配置则选择 `s3`，否则选择 `local`。其他值会使 Hub 启动失败。

用户上传的目录必须以根 `SKILL.md` 为入口，只接受普通文件和安全相对路径。最多 1024 个文件、展开后 512 MiB、Hub 生成的 `tar.zst` 最多 256 MiB。`bin/` 下文件会进入 `skill_exec` 可执行清单，其他文件只读；符号链接、设备、socket、FIFO 和未在 manifest 中声明的内容不会被接受。

领取 Run 时 Hub 固定 Package 快照；替换 Package 不改变在途 Run。Runtime 带 credential 和 Session `ownership_generation` 从 Hub 下载，验证归档及逐文件 checksum 后在 Session staging 中解包，再原子切换 `.pi/agent/skills/` 与 `skill-exec/`。失败时旧物化版本继续保留。旧对象由 Hub 的持久删除队列重试清理，运维不应手工按对象前缀批量删除。

`skill_exec` 只有在 Agent/App 最终工具策略允许且当前 Package 确有 `bin/*` 时才出现。它不经过 shell，只能按当前 Session catalog 精确启动程序；Linux Landlock 将 Package 设为只读，并给每次调用单独的临时 `HOME`/`TMPDIR`。默认超时 30 秒、最大 300 秒，stdin 和 stdout/stderr 各有 1 MiB 限制。非 Linux Runtime 不支持该工具。

## Runtime Engine 镜像升级和回滚

Pi 版本随完整 Runtime 镜像发布。Hub 不分发 candidate、不切换 Runtime 二进制，
也不安排 version checkpoint。

### 无感升级（默认路径）

Runtime 收到 SIGTERM 时不再领取新 Session，并触发与 Drain 相同的保存流程：
立即停止全部 Session 的 Pi RPC 进程，随后在 100 秒总时限内有界地逐个打包生成
Session Bundle 并上传 Hub（每个 Session 的 bundle-sync 状态可经管理台观察），
最后释放 ownership 并退出。Compose 随即启动新镜像，新 Runtime 从 current
Bundle 恢复同一 Pi Session id，活动状态跨升级保留，不需要手工 Drain。超过
100 秒仍未打包的 Session 由新 Runtime 走“无 bundle → 只还原对话”的路径，
对话历史不丢。

升级命令（生产，Runtime 位于 `runtime` profile；三张镜像统一版本后执行）：

```bash
docker compose --profile runtime pull
docker compose --profile runtime up -d --force-recreate
```

开发环境（镜像本地构建，先重建再强制重建容器）：

```bash
docker compose -p agent-hub-dev -f compose.dev.yml up -d --build --force-recreate
```

`--force-recreate` 强制旧 Runtime 容器重建并收到 SIGTERM：即使 `AGENT_HUB_IMAGE_TAG`
复用同一 tag 重新推送（本地镜像 ID 变化但 Compose 配置未变），也能确保切换生效；
省略时若配置与镜像均未变化，Compose 可能保留旧容器继续运行。

两个 Compose 文件均为 `runtime` 服务设置了 `stop_grace_period: 120s`：Compose
发送 SIGTERM 后等待 120 秒才升级为 SIGKILL（未设置时默认只有 10 秒）。该宽限期
必须大于 SIGTERM 触发的打包上传总时限 100 秒，否则时限未到的 Session 会被
SIGKILL 强杀，最新 Workspace 状态无法进入 Bundle。不要在部署中把该值调低到
100 秒以下；手工停止 Runtime 容器（`docker compose stop runtime`）同样受该
宽限期保护，会先完成全部在途 Bundle 再退出。

升级后必须验证 `/healthz`、`/opt/agent-hub/pi/pi --version`、一轮经 fake
或受控 provider 的 Session 对话，以及同一 Session 的下一 Turn continuity。

### 手动 Drain 升级

需要精确控制切换时机（例如维护窗口内主动清空节点、或验证排空后状态）时，仍可
先手工 Drain 再替换镜像。升级前保存当前镜像的不可变 digest 作为上一已知良好
版本。先 Drain 待升级 Runtime，等全部 Session 完成 Turn、提交 current Bundle
并释放 ownership，再用新 digest 重建 Runtime。

任一步失败即停止扩大部署，不 Force Delete 仍可能持有未 checkpoint 状态的旧节点。

回滚同样是整张 Runtime 镜像切换：Drain 新节点并等 Session 排出，固定 Compose 的
Runtime image 为之前记录的 digest，再启动并重复 health、Pi version 和 Session recovery
smoke。回滚镜像会重新物化当前 Agent/Skill/model 配置，但必须 resume Bundle 中同一 Pi
Session id；无法恢复时保留原 Bundle，Session 进入 `recovery_failed`，不得静默创建新
native Session。没有 current Bundle 或仍有未完成 Turn 时，不得把镜像回滚当作数据恢复。

## Runtime drain 和删除

计划维护或下线 Runtime 时：

1. 在 Runtime 管理页核对 hostname 和受影响 Session，执行 Drain。
2. Draining Runtime 不再领取新 Session。活动 Turn 先正常结束；随后每个 Session checkpoint 并释放 ownership。
3. 等受影响 Session 清空后执行普通 Delete。仍持有 Session 时普通删除会被拒绝。

Cancel Drain 只让该 Runtime 恢复领取能力；已经释放并被其他 Runtime 接管的 Session 不会自动迁回。

Force Delete 只用于确认节点永久丢失或无法完成 drain 的情况。它立即撤销 Runtime credential 并使旧 ownership generation 失效。已有 current Bundle 的 Session 可由其他 Runtime 恢复；没有覆盖最新状态的 Bundle 时，确认列表中的 Session 会进入 `recovery_failed`。该操作不可通过旧 credential 撤回。

## `recovery_failed` 处理

`recovery_failed` 表示 Hub 最新历史无法匹配一个完整可恢复的 Workspace 和 native Pi Session，常见原因是永久丢失带有未 checkpoint 数据的 Runtime，或当前 Pi 镜像无法恢复原 Session。

- 保留 Session 历史和最后一个不可变 Bundle，禁止继续发送消息。
- 不回滚到旧 Workspace，不从 Hub 消息重建新 Pi Session，也不自动切回旧 Pi 镜像。
- 先保留对象存储和数据库记录，再检查 Runtime、Hub 日志及当前 Pi 镜像版本。不要用 Force Delete 作为普通重试手段。
- 当前产品没有把 `recovery_failed` Session 原地改回可执行状态的管理操作；它只能作为只读历史保留，或随其 Hub User 被不可逆删除。
