# Agent Hub 项目指令

## 项目概览

Agent Hub 是 Rust 控制面、独立 Rust runtime 与 React 管理台组成的工作区。后端使用 Axum、Tokio、sqlx 和 PostgreSQL；前端使用 React 19、TypeScript、Vite 和 Playwright；本地集成环境由 Docker Compose 提供。

## 目录边界

- `crates/backend/`：HTTP API、认证、调度、PostgreSQL migrations 和后端测试。
- `crates/runtime/`：runtime 注册、任务领取、Pi standalone RPC 驱动和本地运行隔离。
- `crates/shared/`：后端与 runtime 共用的序列化 DTO 和协议类型。
- `frontend/`：React 控制台、API client、样式和 Playwright 测试。
- `compose.yml`：默认生产部署；`compose.dev.yml`：本地开发和测试环境。
- `deploy/`：包含前端静态资源的 Hub 镜像、runtime 镜像及部署辅助脚本。
- `qa/`：无人值守的 API 与浏览器场景；每个 `qa/scenarios/` 子目录是一个独立场景。
- `docs/`：项目规范和功能文档入口。新增架构、接口、运行手册或设计说明放在此目录；Automation 行为见 `docs/automation-spec.md`，认证见 `docs/auth-spec.md`，整体范围见 `docs/v1-spec.md`。

## 环境与启动

工作区声明 Rust 1.88+；Docker 构建镜像使用 Rust 1.91 和 Node 24。启动完整开发环境：

```bash
docker compose -p agent-hub-dev -f compose.dev.yml up -d --build
```

开发 Compose 默认将 Hub 容器的 8080 端口发布到宿主机 15173，可用 `FRONTEND_PORT` 改写宿主机端口。backend 直接托管 `frontend/dist`，默认控制台 URL 为 `http://localhost:15173`。可用 `-p` 或 `E2E_COMPOSE_PROJECT` 使用其他 Compose project name；Playwright 必须指向同一个正在运行的项目。根目录 `compose.yml` 是生产配置，不得用于 E2E。

## 构建与测试

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cd frontend && npm ci && npm run build
E2E_COMPOSE_PROJECT=agent-hub-dev npm run test:e2e
./qa/run-all.sh
```

需要真实 PostgreSQL 的 ignored `#[sqlx::test]` 使用具有 `CREATE DATABASE` 权限的 `DATABASE_URL`，并通过 `cargo test -p agent-hub-backend -- --ignored` 运行。执行前应先确认 URL 指向测试数据库，不得指向生产库。

## 发布与 CI 准则

每次发布新版本时，必须在一个版本变更中完成以下内容：

1. `VERSION` 改为新的 `MAJOR.MINOR.PATCH`。
2. Rust 版本：
   - `crates/backend/Cargo.toml`
   - `crates/runtime/Cargo.toml`
   - `crates/shared/Cargo.toml`
   - `crates/agent-hub-cli/Cargo.toml`
   - `Cargo.lock`

   修改 manifest 后运行 `cargo generate-lockfile`，不得用 `cargo update` 顺便升级无关依赖。
3. 前端版本：
   - `frontend/package.json`
   - `frontend/package-lock.json`

   运行 `npm install --package-lock-only --ignore-scripts --prefix frontend` 更新 lockfile；不要提交 `frontend/dist`。
4. TypeScript SDK 版本：
   - `sdk/typescript/package.json`
   - `sdk/typescript/package-lock.json`

   运行 `npm install --package-lock-only --ignore-scripts --prefix sdk/typescript`，随后运行 SDK build。若受版本发布的构建影响，提交重新生成的 `sdk/typescript/dist/`。
5. 生产镜像默认版本：
   - `compose.yml` 中 gateway、backend、runtime-init、runtime 四处的 `${AGENT_HUB_IMAGE_TAG:-X.Y.Z}`。
   - `.env.example` 的 `AGENT_HUB_IMAGE_TAG=X.Y.Z`。

   生产默认值必须固定语义化版本，不得默认 `latest`。
6. 运维文档：
   - `docs/operations.md` 中三张默认镜像的版本表格。
   - `docs/operations.md` 中 `gh workflow run Release ... -f release_tag=vX.Y.Z` 示例。
7. 用户可见发布记录：
   - `CHANGELOG.md` 增加新版 `## [X.Y.Z]` 章节。
   - `RELEASE_NOTES.md` 标题改为 `# 🚀 Agent Hub vX.Y.Z`，更新发布日期和本次发布内容。
8. 默认启动和安装文档不得保留历史版本作为用户默认路径。

版本文件全部更新后，至少运行：

```bash
bash scripts/verify-release-version.sh
cargo fmt --all --check
```

CI 和 Release workflow 都必须复用 `.github/workflows/quality.yml`。版本变更必须先合入 `main` 并通过质量门禁，再创建 `vX.Y.Z` tag；发布后不得移动或覆盖已有 tag。所有 PR 和 `main` 分支必须经过 CI，不得跳过、删除或放宽 `fmt`、Clippy、workspace tests、PostgreSQL/MinIO ignored tests、SDK、frontend 和 Go gateway 检查。

架构图、README 和 specs 必须与当前协议一致。模型请求的权威链路是 Runtime → Hub Responses proxy → Model Gateway → provider；不得再画出 Runtime 直接访问 Gateway/provider 的旧链路。CI 新增的数据库测试必须使用独立 Postgres/MinIO 服务，不得连接生产数据库或把生产凭据写入 workflow、环境变量或日志。


## 代码风格

- Rust 使用 edition 2021，提交前运行 `cargo fmt` 和严格 Clippy；错误需保留必要上下文，公共 DTO 保持 serde 契约清晰。
- TypeScript 使用严格类型检查；沿用 React function component、hooks 和 `frontend/src/api/client.ts` 的 API 边界。用户可见文案统一放在 `frontend/src/i18n.ts`，不得创建页面级重复词典。
- 遵循现有锁顺序和事务边界。Automation、Agent 归档和触发路径均先锁 Agent，再锁 Automation。
- 只修改任务要求的模块，不重构或格式化无关文件。

## 验证与安全

- 运行与改动风险相匹配的最小新鲜验证；API 契约变化同步 shared DTO、OpenAPI、client 和数据库测试。
- UI 改动至少验证桌面与 390px 移动视口、浏览器 console/network、无横向溢出和关键真链。
- 明文 API key、webhook token、OAuth secret、session 和模型 provider secret 仅在协议规定的一次性响应中展示；不得写日志、测试快照、URL 查询参数或文档示例中的真实值。
- 不运行 `git reset --hard`、`git checkout --`、`git clean`，不回退不属于当前任务的改动。
