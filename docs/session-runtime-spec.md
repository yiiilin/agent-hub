# Session Runtime Spec

## 目的与边界

本文件集中定义 Hub Session、Runtime ownership、Native Session、Session Bundle 和 Runtime Engine 版本切换的执行契约。ADR-0001 至 ADR-0021 与本文件优先于仍以 Hub Run 描述执行历史的早期文档。

## Pi 执行实现

当前 Runtime 使用 Pi standalone RPC。Hub 的公开 DTO 和数据库使用
`engine_version` 表示 Runtime Engine 版本，使用 `native_session_id` 表示 Pi
Native Session ID；Bundle 的 `native-session/` 保存 Pi recovery JSONL。精确 Pi
进程、RPC、模型代理、工具和 Bundle 约束见
[`pi-driver-spec.md`](pi-driver-spec.md)。

```text
Hub User 1 --- many Hub Sessions
Agent    1 --- many Hub Sessions (binding is immutable)

Hub Session 1 --- 1 Workspace
            1 --- 1 native Pi Session
            1 --- many ordered Messages
            1 --- many Hub Runs (scheduling/audit)
            1 --- 0..1 active Native Turn
```

- Hub-native Session 的 external origin 全部为空。
- External Session 的 External Platform、Tenant 和 Identity 全部固定且不可部分为空。
- Hub 管理台可查看当前用户拥有的全部 Session 历史，但只允许在 Hub-native Session 中发送消息、立即引导和停止 Turn。External Session 在管理台始终只读；与其完整 origin 匹配的外部 token 仍可通过外部集成接口继续对话。
- Session 创建后不能更换 owner 或 Agent。
- 管理台为每个 Hub User 与可调用 Agent 组合最多保留一个 Conversation Draft。Draft 及输入内容只保存在当前浏览器的 `localStorage`，刷新和关闭浏览器后仍保留；显式丢弃只删除当前 Draft，显式退出登录清除该用户在当前浏览器中的全部 Draft。
- “新建会话”不打开表单，也不创建后端记录，而是切回 Hub-native 来源并打开所选 Agent 已有的 Draft，或创建一个空 Draft。只有首条消息被接受时，Hub 才在同一事务中创建 Hub-native Session、Message 和 Run；成功后清除该 Draft，失败时保留 Draft 及输入内容。
- Conversation Draft 不进入 Session 列表，也不分配 Session、Run、Runtime、Workspace、Runtime ownership 或 native Pi Session。

## 管理台会话导航

- 会话侧栏控件固定按“平台、Agent、搜索”排列。平台默认选择“本平台”，另提供“全部平台”，并按名称逐个列出有 Session 的 External Platform；不使用笼统的“外部平台”选项。
- Agent 始终选择一个具体 Agent，不提供“全部智能体”。可调用 Agent 同时过滤 Session 列表并决定“新建会话”打开哪个 Conversation Draft；只存在于已有 Session 的已删除或不可调用 Agent 仍作为仅查看选项保留，但不能新建 Draft。管理台按 Hub User 恢复上次有效选择，失效时优先回退到第一个可调用 Agent，否则回退到已有 Session 的 Agent。
- 平台筛选仅影响正式 Session 列表，不把 Conversation Draft 插入列表。查看 External Platform 时点击“新建会话”，平台自动切回“本平台”。
- External Session 继续按消息和活动事件的原始顺序展示历史，但不展示消息输入框、立即引导或停止操作。Hub 的 console message 与 stop API 也必须拒绝 External Session，不能只依赖前端隐藏控件。

## Session 状态机

持久化状态使用以下互斥语义；API 可以另带 active/pending Turn、drain 和错误详情，但不得用这些字段改变状态含义。

| 状态 | 含义 | 可执行新 Turn |
| --- | --- | --- |
| `waiting_for_runtime` | 有待处理消息，但没有可取得 ownership 的 Runtime | 否，消息继续排队 |
| `restoring` | Runtime 已取得新 generation，正在准备本地目录、校验/解包 Bundle、重新物化配置并 resume Pi Session | 否，普通消息按顺序加入即将开始的 Turn |
| `online` | 当前 owner 持有完整本地目录；可处于 idle 或恰有一个 active Turn | 是 |
| `saving` | Pi RPC 进程已停止，Runtime 正在创建或经 Hub 上传 Bundle | 否，新消息持久化并排队，不能修改本次 checkpoint |
| `offline` | current Bundle 已提交，Runtime ownership 已释放，本地目录不再是权威恢复来源 | 收到消息后先分配 Runtime |
| `recovery_failed` | Bundle 无法恢复，或 Hub 历史新于最后成功 Bundle 且原节点已永久丢失 | 否，仅可查看历史和最后 Bundle 信息 |

允许的主要转换：

```text
waiting_for_runtime -> restoring -> online
offline             -> restoring -> online
online(active)      -> online(idle)              # Turn terminal
online(idle)        -> saving                     # 15 min idle, drain, or engine image switch
saving              -> offline                    # Bundle committed and ownership released
saving              -> online                     # queued work reuses retained local files
restoring|saving    -> recovery_failed            # only the specified unrecoverable cases
```

- 新 Session 没有 Bundle 时，`restoring` 准备空 Session 目录并启动新 Pi Session；已有 Bundle 时恢复唯一 JSONL，并用 `--session <saved-jsonl>` resume。
- Turn 进入任意终态后开始十五分钟 idle timer。新 Turn 取消 timer；浏览器在线不续期；active Turn 永不因 idle 被停止。
- `saving` 成功且有本地待处理消息时，非 draining Runtime 可复用仍保留的本地目录启动 Pi，无需下载刚上传的 Bundle。
- `saving` 失败时保留本地目录和旧 current Bundle：无待处理消息自动重试；有待处理消息则结束保存、恢复本地执行，并在下一 idle 边界再保存。
- Runtime drain 和版本切换覆盖十五分钟等待：idle Session 立即保存；active Turn 正常结束后保存，不启动下一 Turn。
- ordinary Runtime 删除必须等所有 Session 保存并释放；force delete 立即撤销凭据和 generations。具有 current Bundle 的 Session 可重新分配，没有 current checkpoint 的最新 Session 进入 `recovery_failed`。

## 消息与 Turn 顺序

1. 每个 Session 内，Hub 为每条已接受消息分配严格递增的顺序；消息不可合并、覆盖或因重试重复。
2. 没有 active Turn 时，普通消息保持各自历史记录，并按顺序作为同一个下一 Pi `prompt` 的 input；明确选择“稍后处理”的消息保留给再下一个 Turn。
3. Pi `turn_start` 后，本次 Hub Run 与 Runtime 生成的 native Turn ID 绑定。之后到达的立即引导消息使用 Pi `steer`，仍作为独立历史消息并归入同一 Hub Run/Turn。
4. 如果 Pi steer 到达时 expected Turn 已结束，Hub 把该消息恢复为下一 Turn 的首批 input；不得改用当前看到的另一个 Turn ID 重试。
5. 显式停止立即发送 Pi `abort`。已完成命令、工具事件、Workspace 修改和外部副作用不撤销；最终以 interrupted 记录。
6. 在 `restoring` 或 `saving` 中接受的消息先持久化再排队；恢复完成前的普通消息按顺序进入同一个 upcoming Turn。
7. 一个 Session 同时最多有一个 active Turn 和一个执行它的 Runtime owner；Run 重试不得造成双 Turn。
8. 管理台恢复 Session 历史时必须加载该 Session 消息所关联的全部 Run 事件，不得只加载最后一个 Run；Run 进入终态后重新读取持久化消息状态，避免继续显示已经送达的 `queued` 状态。
9. Runtime 将 Pi thinking、命令和工具事件映射为可读活动事件。Thinking 只持久化可显示摘要；本迭代不启用 MCP。管理台不把 `status`、`usage`、`turn_started`、message delta 等内部事件原样展示给用户。

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
  engine-state/     # Session 专属 Pi HOME
    .pi/agent/       # Hub 可重建 instructions、models 和 Skills
    sessions/        # Pi native Session JSONL
  supervisor/       # owner/generation、进程和恢复所需的本地元数据
  staging/          # 临时 Bundle 文件；不属于归档内容
```

- 不同 Session 不共享可写 Workspace、Pi HOME、生成配置或 native JSONL。
- Agent instructions、Skills 和 Model Connection references 由 Hub 数据重新生成，只在 Turn 之间按当前 fingerprint 写入；活动 Turn 保持稳定文件集，真实 provider key 永不进入 Session 目录。本迭代不物化 MCP 或子 Agent。
- Skill 物理删除后，Hub 为受影响在线 Session 发出 generation-fenced `refresh_configuration`，携带当前完整配置与 fingerprint。空闲 Session 立即原子 materialize；活动 Turn 只记住命令，到终态后处理并回执。
- 过期 refresh 回执不得清除更新的待刷新状态；下一 heartbeat 继续下发最新 fingerprint。刷新不重启 Pi，不写 Workspace。
- Runtime 进程重启后从持久化目录发现仍由自己拥有的在线 Session；不得仅因 Runtime 进程退出就丢弃目录。

## Session Bundle

Bundle 是 streaming `tar.zst`，顶层必须且只能包含：

```text
workspace/       # 完整 Workspace，包含隐藏文件和 .git，不按 .gitignore 过滤
manifest.json    # 格式/Hub Session/Pi Session/history checkpoint/generations/version/size/checksum
native-session/  # 只含 sessions/<one Pi JSONL>
```

- 打包前停止 Pi RPC 进程，确保 JSONL 已落盘。
- 创建端解析每个直接位于 Pi `sessions/` 下的 `.jsonl` 首行，只选择 `type=session` 且 `id` 等于 manifest native Session id 的唯一文件；文件名不参与身份判断。
- 恢复端只接受 `native-session/`、`native-session/sessions/` 和一个直接子级 `.jsonl`，并在提交目录前验证该 JSONL header 与 manifest 匹配。
- 排除 `.pi/agent`、Hub 认证、model proxy token、Runtime Credential、logs、caches、settings、extensions、Skills、Pi binary、其他 Session JSONL 和可由 Hub 重建的配置。
- 普通文件、目录和不会逃逸归档根的安全 symlink 可进入 Bundle；拒绝 device、socket、FIFO、路径穿越和逃逸链接。
- Runtime 流式计算压缩 Bundle checksum、压缩大小和内容声明；恢复 Runtime 在解包前验证。Hub 不 unpack、scan、hash 或完整缓冲 Bundle。
- 每个 Session 只保留一个成功 generation。新对象完整写入后，Hub 在校验 current ownership generation 和小型 commit metadata 后原子切换 current pointer，再删除旧对象；失败上传永不成为 current。
- 默认压缩大小上限 10 GiB，可由管理员配置。

## Hub 与网络边界

- Runtime 的注册/heartbeat、Session command/event 和 Bundle 上传/下载等系统流量只与 Hub 通信。Pi artifact 随 Runtime 镜像交付，不由 Runtime 下载。
- Pi Responses 请求经 Runtime loopback proxy 到 Hub，再由 Hub 使用数据库中的 Model Connection 访问 provider；Runtime 不持有 provider URL credential，也没有 direct fallback。
- Bundle 上传为 Runtime -> Hub -> S3-compatible storage；下载反向经过 Hub。Runtime 不获得 S3 credential、bucket、object URL 或 signed URL。
- Hub 对 Bundle body 只做带 backpressure 的流式转发和大小限制，不承担内容计算。中断传输从头重试。
- S3-compatible endpoint 可配置 HTTP 或 HTTPS；server-side encryption 是可选部署配置。
- Pi 在执行任务时的网络访问不属于 Runtime 系统流量，受 Runtime 工具 allowlist、非 root 容器和部署网络边界约束。

## Runtime Engine 版本切换

- Pi 版本固定在 Runtime image 中，并以 `engine_version` 上报。不可变版本、源码 commit、build patch、Bun baseline 和 model-data snapshot 必须一起验证。
- Hub 不提供 Runtime Engine rollout API，Runtime 不下载或切换执行二进制。
- 升级或回滚以完整 Runtime image 为单位。先 Drain 并等 Session current Bundle 提交、ownership 释放，再替换镜像；active Turn 不被混用版本。
- 新镜像恢复时重新物化当前配置并 resume Bundle 中同一 Pi Session id。无法 resume 时保留原 Bundle 和只读 Hub 历史，Session 进入 `recovery_failed`，不得创建替代 native Session。

## 验收测试边界

- 数据库：origin 完整性、消息顺序、Session/Agent 不可变绑定、ownership generation fencing 和 current Bundle 原子切换。
- Runtime：目录隔离、一个 Native Session 多 Turn、steer/interrupt race、进程重启恢复、idle/drain/version 时序、Skill refresh 空闲/活动/过期命令和安全 tar.zst。
- Hub streaming：认证、generation、size limit、backpressure、中断传输和 S3-compatible HTTP/HTTPS 配置。
- 版本：固定 submodule/model-data/patch checksum、baseline standalone 构建、最终镜像无 Node/npm/Bun，以及整镜像 rollback 流程。
- 浏览器：desktop 与 390px 下的会话列表和来源筛选、Agent 选择新建对话、SSE 实时回复、可读 Pi 活动折叠、多 Turn 历史恢复、Historical Session 只读、立即引导、显式停止和 Runtime drain/delete。
