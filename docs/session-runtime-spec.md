# Session Runtime Spec

## 目的与边界

本文件集中定义 Hub Session、Runtime ownership、native Codex Thread、Session Bundle 和 Codex 版本切换的执行契约。ADR-0001 至 ADR-0021 与本文件优先于仍以 Hub Run 描述执行历史的早期文档。

```text
Hub User 1 --- many Hub Sessions
Agent    1 --- many Hub Sessions (binding is immutable)

Hub Session 1 --- 1 Workspace
            1 --- 1 native Codex Thread
            1 --- many ordered Messages
            1 --- many Hub Runs (scheduling/audit)
            1 --- 0..1 active Codex Turn
```

- Hub-native Session 的 external origin 全部为空。
- External Session 的 External Platform、Tenant 和 Identity 全部固定且不可部分为空。
- Session owner 可通过 Hub 控制面管理自己的 Session；外部 token 只能访问与自身完整 origin 相同的 Session。
- Session 创建后不能更换 owner 或 Agent。

## Session 状态机

持久化状态使用以下互斥语义；API 可以另带 active/pending Turn、drain 和错误详情，但不得用这些字段改变状态含义。

| 状态 | 含义 | 可执行新 Turn |
| --- | --- | --- |
| `waiting_for_runtime` | 有待处理消息，但没有可取得 ownership 的 Runtime | 否，消息继续排队 |
| `restoring` | Runtime 已取得新 generation，正在准备本地目录、校验/解包 Bundle、启动 app-server 并 resume Thread | 否，普通消息按顺序加入即将开始的 Turn |
| `online` | 当前 owner 持有完整本地目录；可处于 idle 或恰有一个 active Turn | 是 |
| `saving` | app-server 已停止，Runtime 正在创建或经 Hub 上传 Bundle | 否，新消息持久化并排队，不能修改本次 checkpoint |
| `offline` | current Bundle 已提交，Runtime ownership 已释放，本地目录不再是权威恢复来源 | 收到消息后先分配 Runtime |
| `recovery_failed` | Bundle 无法恢复，或 Hub 历史新于最后成功 Bundle 且原节点已永久丢失 | 否，仅可查看历史和最后 Bundle 信息 |

允许的主要转换：

```text
waiting_for_runtime -> restoring -> online
offline             -> restoring -> online
online(active)      -> online(idle)              # Turn terminal
online(idle)        -> saving                     # 15 min idle, drain, or version switch
saving              -> offline                    # Bundle committed and ownership released
saving              -> online                     # queued work reuses retained local files
restoring|saving    -> recovery_failed            # only the specified unrecoverable cases
```

- 新 Session 没有 Bundle 时，`restoring` 准备空 Session 目录并使用 `thread/start`；已有 Bundle 时校验后使用 `thread/resume`。
- Turn 进入任意终态后开始十五分钟 idle timer。新 Turn 取消 timer；浏览器在线不续期；active Turn 永不因 idle 被停止。
- `saving` 成功且有本地待处理消息时，非 draining Runtime 可复用仍保留的本地目录启动 app-server，无需下载刚上传的 Bundle。
- `saving` 失败时保留本地目录和旧 current Bundle：无待处理消息自动重试；有待处理消息则结束保存、恢复本地执行，并在下一 idle 边界再保存。
- Runtime drain 和版本切换覆盖十五分钟等待：idle Session 立即保存；active Turn 正常结束后保存，不启动下一 Turn。
- ordinary Runtime 删除必须等所有 Session 保存并释放；force delete 立即撤销凭据和 generations。具有 current Bundle 的 Session 可重新分配，没有 current checkpoint 的最新 Session 进入 `recovery_failed`。

## 消息与 Turn 顺序

1. 每个 Session 内，Hub 为每条已接受消息分配严格递增的顺序；消息不可合并、覆盖或因重试重复。
2. 没有 active Turn 时，普通消息保持各自历史记录，并按顺序作为同一个下一 `turn/start` 的 input；明确选择“稍后处理”的消息保留给再下一个 Turn。
3. `turn/start` 成功返回的 native Turn ID 与本次 Hub Run 绑定。之后到达的普通消息使用 `turn/steer(threadId, expectedTurnId, input)`，仍作为独立历史消息并归入同一 Hub Run/Turn。
4. 如果 Codex 明确拒绝 steer，原因是 expected Turn 已结束，Hub 把该消息恢复为下一 Turn 的首批 input；不得改用当前看到的另一个 Turn ID重试。
5. 显式停止立即发送 `turn/interrupt(threadId, turnId)`。已完成命令、工具事件、Workspace 修改和外部副作用不撤销；最终以 interrupted 记录。
6. 在 `restoring` 或 `saving` 中接受的消息先持久化再排队；恢复完成前的普通消息按顺序进入同一个 upcoming Turn。
7. 一个 Session 同时最多有一个 active Turn 和一个执行它的 Runtime owner；Run 重试不得造成双 Turn。

## Runtime Ownership

- Runtime 取得 Session 时，Hub 在 PostgreSQL 事务中设置 owner 并递增 `ownership_generation`；generation 单调递增且永不复用。
- Session command（包括 `refresh_configuration`）、Run/Turn/Item event、heartbeat-owned 状态更新、保存结果和 Bundle commit 全部携带 generation；Hub 拒绝非当前 owner 或旧 generation 的写入。
- heartbeat 丢失不自动把可能只有本地最新数据的 Session 改派给别的 Runtime。节点恢复后继续原 generation；管理员确认永久丢失时按 force delete/recovery-failed 规则处理。
- draining Runtime 不取得新 Session。已 active 的 Turn 和 Steering Messages 可结束，但不启动 queued later Turn；提交 current Bundle 后释放 ownership。
- 取消 drain 只允许该 Runtime 再次接活，不把已经释放的 Session 自动迁回。

## Runtime 本地目录

每个在线 Session 使用独立持久化目录，至少隔离以下职责：

```text
session-root/
  workspace/        # 用户可见、跨 Turn 延续的工作区
  codex/            # Session 专属 CODEX_HOME；包含生成配置和 native Thread 状态
  supervisor/       # owner/generation、进程和恢复所需的本地元数据
  staging/          # 临时 Bundle 文件；不属于归档内容
```

- 不同 Session 不共享可写 Workspace、Codex 目录、认证文件或 native Thread 文件。
- Agent/Skill/MCP/Model Connection references/Codex Subagent config 由 Hub 数据重新生成，只在 Turn 之间且 fingerprint 改变时写入；活动 Turn 保持稳定文件集，真实 provider key 永不进入 Session 目录。
- Skill 物理删除后，Hub 为受影响在线 Session 发出 generation-fenced `refresh_configuration`，携带当前完整配置与 fingerprint。空闲 Session 立即原子 materialize；活动 Turn 只记住命令，到终态后处理并回执。
- 过期 refresh 回执不得清除更新的待刷新状态；下一 heartbeat 继续下发最新 fingerprint。刷新不重启 app-server，不写 Workspace。
- Runtime 进程重启后从持久化目录发现仍由自己拥有的在线 Session；不得仅因 Runtime 进程退出就丢弃目录。

## Session Bundle

Bundle 是 streaming `tar.zst`，顶层必须且只能包含：

```text
workspace/       # 完整 Workspace，包含隐藏文件和 .git，不按 .gitignore 过滤
manifest.json    # 格式/Session/Thread/history checkpoint/generations/version/size/checksum
codex-thread/    # resume 该 Thread 所需的最小 native transcript 与 index 快照
```

- 打包前停止 app-server，确保 transcript/index 一致。
- 排除 Codex/Hub 认证、model proxy token、MCP secret、Runtime Credential、logs、caches、shell snapshots、Skills 和可由 Hub 重建的配置。
- 普通文件、目录和不会逃逸归档根的安全 symlink 可进入 Bundle；拒绝 device、socket、FIFO、路径穿越和逃逸链接。
- Runtime 流式计算压缩 Bundle checksum、压缩大小和内容声明；恢复 Runtime 在解包前验证。Hub 不 unpack、scan、hash 或完整缓冲 Bundle。
- 每个 Session 只保留一个成功 generation。新对象完整写入后，Hub 在校验 current ownership generation 和小型 commit metadata 后原子切换 current pointer，再删除旧对象；失败上传永不成为 current。
- 默认压缩大小上限 10 GiB，可由管理员配置。

## Hub 与网络边界

- Runtime 的注册/heartbeat、Session command/event、Codex binary 获取和 Bundle 上传/下载等系统流量只与 Hub 通信。
- Codex Responses 请求经 Runtime loopback proxy 到 Hub，再由 Hub 使用数据库中的 Model Connection 访问 provider；Runtime 不持有 provider URL credential，也没有 direct fallback。
- Bundle 上传为 Runtime -> Hub -> S3-compatible storage；下载反向经过 Hub。Runtime 不获得 S3 credential、bucket、object URL 或 signed URL。
- Hub 对 Bundle body 只做带 backpressure 的流式转发和大小限制，不承担内容计算。中断传输从头重试。
- S3-compatible endpoint 可配置 HTTP 或 HTTPS；server-side encryption 是可选部署配置。
- Codex 在执行任务时的网络访问不属于 Runtime 系统流量，严格遵循 Agent sandbox/network policy。

## Codex 精确版本切换

- 全平台只有一个具体的 Target/Active Codex Version；不得激活可变的 `latest`。
- Hub 从官方 GitHub release 为每个已注册架构下载 artifact、验证发布完整性并只向已认证 Runtime 分发；Runtime 不直接访问 GitHub。
- Runtime 验证 artifact，运行有界基础兼容检查，按具体版本保存并报告 readiness。满足平台策略后 Hub 才把该具体版本提升为 Active。
- active Turn 始终使用启动它的旧进程直至终态，不在一个 Turn 中混用版本。
- promotion 后，旧版本 Session 在 Turn 结束后立即停止 app-server，并以旧 producing version 创建 current Bundle；下一 Turn 直接使用新 Active 版本恢复。
- 不执行旧 Bundle 到 candidate 的跨版本恢复测试，不为单个 Session 保留旧 binary fallback，也不因恢复失败静默创建新 Thread。
- 新版本无法 resume 时保留原 Bundle和只读 Hub 历史，Session 进入 `recovery_failed`。

## 验收测试边界

- 数据库：origin 完整性、消息顺序、Session/Agent 不可变绑定、ownership generation fencing 和 current Bundle 原子切换。
- Runtime：目录隔离、一个 Thread 多 Turn、steer/interrupt race、进程重启恢复、idle/drain/version 时序、Skill refresh 空闲/活动/过期命令和安全 tar.zst。
- Hub streaming：认证、generation、size limit、backpressure、中断传输和 S3-compatible HTTP/HTTPS 配置。
- 版本：架构映射、artifact 完整性、基础检查、全平台 readiness 和 active Turn 不受 promotion 干扰。
- 浏览器：desktop 与 390px 下的会话列表和来源筛选、Agent 选择新建对话、SSE 实时回复、技术事件折叠、Historical Session 只读、立即引导、显式停止和 Runtime drain/delete。
