# Comprehensive QA Coverage Spec

## 目标

Agent Hub V1 的每项功能必须能追溯到自动化证据。QA 不要求把所有分支重复放进浏览器，而是按风险选择 Rust、API 或浏览器层，并保证每个核心功能域至少有一条使用真实 Compose 服务的无人值守链路。

权威功能范围来自：

- `docs/*-spec.md` 中仍有效的范围与验收标准；
- `/openapi.json` 暴露的公共、用户、集成和 Runtime API；
- 管理台当前可达页面、对话 Widget 和嵌入协议；
- `docs/session-runtime-spec.md` 定义的 Session、Runtime、Bundle 和 Execution Engine 生命周期。

文档明确列为非目标的能力不进入覆盖目录。

## 覆盖层级

| 层级 | 主要职责 |
| --- | --- |
| Rust | 纯逻辑、序列化、数据库约束、协议状态机、并发与安全边界 |
| API QA | 真实 PostgreSQL、Hub、Runtime、Pi standalone 与 model gateway，以及 fake model provider、Mock OIDC 和 MinIO 边界替代的契约与负路径 |
| Browser QA | 真实前端与后端关键工作流、交互状态、desktop/390px、i18n、console/network |

覆盖完成必须同时满足：

1. `qa/features.json` 中没有缺失自动化证据的功能。
2. 每个核心功能域至少被一个 API 或 Browser QA 场景覆盖。
3. 所有用户可见页面和关键 Widget 交互至少被一个 Browser QA 场景覆盖。
4. OpenAPI 中的每个 operation 都映射到功能目录；新增 operation 未映射时覆盖门禁失败。
5. 场景清单不得引用不存在的功能编号，功能证据不得引用不存在的测试文件或测试标识。

覆盖编号只表示可验收行为，不表示源文件、数据库表或实现任务。一个场景可以覆盖多个相关行为，一个行为也可以由多个层级共同覆盖。

## 功能域

- Platform bootstrap、health/readiness、OpenAPI 和导航。
- Password、Mock OIDC、browser session、API Key 和 Embed JWT 认证。
- Administration 用户、角色、认证策略、External Platform 和 Authentication Channel。
- Agent 生命周期、visibility、Runtime 约束、Markdown、Subagent Definition 和历史只读。
- Hub-managed Skill、Agent 绑定、批量删除、refresh fencing 和 MCP secret。
- Global/Personal Model Connection、System Default、透明 Responses proxy、usage/error ledger。
- Session、Message、Run、Turn、SSE、steer、interrupt、Native Session 和 Workspace 隔离。
- Runtime enrollment、credential、ownership、drain/delete、Session Bundle、recovery 和 Runtime Engine 版本。
- Automation CRUD、manual/webhook/interval/cron、scheduler 和 Run history。
- Integration App、OAuth、External Session、Client Access Credential、SSE、attachment、Run Tool Snapshot、Client Tool claim/result/batch/timeout 和权限失效。
- Browser SDK 与 Widget 的认证/匿名初始化、Origin 隔离、多标签页、session select、message idempotency、SSE resume、续期/重新授权、IndexedDB 防重放、stop 和 `postMessage`。
- 所有管理台页面的 desktop/390px、英文/中文、loading/error/empty 和浏览器诊断。

## 场景契约

每个 `qa/scenarios/<id>/` 包含 `scenario.json`、`scenario.mjs` 和 `README.md`。Manifest 必须声明：

- 稳定场景名称和 `api|browser` 类型；
- 硬超时；
- 该场景直接验证的功能编号；
- 需要时声明全局设置恢复责任。

场景必须使用唯一测试数据，不依赖前一个场景留下的数据。普通删除只能删除场景自己创建的资源。修改全局设置的场景排在最后，并在 `finally` 中恢复；恢复失败本身属于场景失败。

默认 `./qa/run-all.sh` 运行完整场景集。`--type api` 不得导入 Playwright 或启动 Chromium。单场景执行仍使用一套全新的 Compose 环境。

## 测试环境

全部场景运行真实 Hub、Runtime、内嵌 Pi standalone、PostgreSQL 和 model gateway；外部边界使用本地 fake model provider、Mock OIDC 和 MinIO，不访问真实 AI、OAuth、S3 或 GitHub 服务。不得为测试新增绕过生产认证或授权的后门。

每次可执行场景运行都先执行 TypeScript SDK 的 `npm test`、`npm run build` 和 `npm pack --dry-run`。任一 SDK 门禁失败都使整次 QA 失败，但仍可继续收集所选场景的独立证据。

一次完整运行只启动一套隔离 Compose 环境。普通场景失败后继续运行其余场景；若 worker 触发 OS-level hard timeout，则当前场景记为 `failed`，后续已选择场景全部记为 `not_run` 并注明共享环境可能已污染，且不得再启动 worker。随后仍按正常流程 teardown 这一套 Compose；运行结束或收到信号时删除容器、网络和 QA 数据卷。暖缓存完整运行目标不超过十分钟。

## 诊断和产物

每次运行生成 `summary.json`、`junit.xml`、SDK 门禁日志和 SHA-256 `artifact-manifest.json`。Summary 记录源码 revision、执行前后 dirty-tree fingerprint、时间窗口、`owned_ephemeral` 环境身份与实际 teardown disposition、依赖的 real/emulated/mocked 模式，以及每个场景实际执行的 claim 记录。失败场景保存脱敏错误和 Compose 日志；浏览器失败另存 screenshot、browser diagnostics 和 Playwright trace。Browser QA 将未允许的同源 `4xx/5xx`、request failure、page error 和 console error 视为失败。最终保留树必须通过递归脱敏与 secret/artifact safety 扫描，扫描结论写入 Summary。

产物不得包含 session、API Key、OAuth/client secret、Runtime Credential、model provider key、MCP secret 或 webhook token 明文。

## 停止条件

出现以下情况时停止实施并重新确认：

- 自动化需要削弱认证、权限、secret 或 Session 隔离边界；
- 必须新增只在生产 API 中暴露的测试控制入口；
- 已确认的产品契约与代码行为发生实质冲突；
- 共享环境无法可靠恢复，必须改成每场景重启 Compose；
- 暖缓存全量持续超过十分钟，需要改变默认门禁或并行模型。
