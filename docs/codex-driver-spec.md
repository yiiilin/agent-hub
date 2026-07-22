# Codex App-Server Driver 功能链 Spec

## 范围

本文件定义 Runtime 驱动 Codex app-server 的目标契约；Session 生命周期与恢复细节以 `docs/session-runtime-spec.md` 为准：

1. Runtime 支持 `RUNTIME_CODEX_DRIVER=fake|app-server`；`app-server` 模式启动 Hub 分发并固定到具体版本的 `CODEX_BIN app-server --listen stdio://`。
2. 一个在线 Session 独占一个 app-server 子进程和一条 stdio 连接；连接只执行一次 `initialize`/`initialized` 握手，不因 Hub Run 结束而退出。
3. 新 Session 使用 `thread/start` 创建一个 native Codex Thread；从 Bundle 恢复时使用 `thread/resume` 继续同一个 Thread。一个 Hub Session 不得静默更换 Thread。
4. Session 空闲时的普通消息通过 `turn/start` 开始新 Turn；活动 Turn 中的普通消息通过带 `expectedTurnId` 的 `turn/steer` 立即引导；显式停止通过带 `threadId` 和 `turnId` 的 `turn/interrupt` 中断且不回滚。
5. Runtime 读取 app-server 的 response/notification，并把 `item/*`、`thread/tokenUsage/updated`、`turn/completed` 等事件映射到正确的 Hub Session、Run 和 Turn。请求 response 必须按原请求 `id` 关联，不能只按到达顺序猜测。
6. Runtime 持久化 native Thread ID；Hub Run 仅用于调度和审计，不充当 Codex Thread、Workspace 或进程边界。
7. app-server 子进程的超时、取消和异常路径必须确定性回收；`app-server` 失败不得静默回退到 fake driver。
8. Turn 结束后，Session 默认保持在线十五分钟；新 Turn 复用同一进程。仅在保存 Bundle、Runtime drain、版本切换或错误回收时停止进程。
9. Agent、Skill、Model Connection reference、详细 Codex 参数、协议专属请求参数和 Codex Subagent files 只在 Turn 之间按有效配置 fingerprint 同步；同步文件本身不重启 app-server。已加载 native Thread 的 fingerprint 变化时，Runtime 在下一 Turn 前先执行 `thread/unsubscribe`，再对同一 Thread 执行强制 cold `thread/resume` 以重读最新配置，最后才执行 `turn/start`。Skill 删除产生的 `refresh_configuration` 在 idle 立即执行，active Turn 终态后执行。
10. Runtime 为默认模型和子 Agent override 生成多个受控 provider/agent 配置，写入所选连接的 reasoning、summary、verbosity、context、compaction、summary capability、service tier 及 provider retry/idle 参数；自动值省略。协议专属请求参数不写入 Codex TOML，Runtime 对每个连接始终发送 Responses 请求，Hub/Gateway 在请求时处理上游协议。所有 provider 都指向 Runtime loopback proxy，真实 provider URL/API Key 不写入 `CODEX_HOME`。
11. Runtime 的系统流量只访问 Hub。Codex 任务产生的网络流量遵循 Agent sandbox；Runtime 不直接访问 provider、GitHub 或 S3。

## 协议顺序

```text
connect -> initialize -> initialized
        -> thread/start | thread/resume
        -> turn/start
        -> turn/steer (0..n, only for the expected active Turn)
        -> turn/interrupt (explicit stop) | turn/completed
        -> turn/start (next user round, same Thread)
```

- `thread/resume` response 返回原 Thread。
- `turn/start` response 返回 Codex 生成的 active Turn ID。
- `turn/steer` 必须携带 `threadId`、`expectedTurnId` 和非空 `input`，response 返回同一个 `turnId`。
- `turn/interrupt` 必须携带 `threadId` 和 `turnId`，response 成功后仍以 `turn/completed(status=interrupted)` 作为 Turn 终态通知。
- 如果 `turn/steer` 因预期 Turn 已结束而被拒绝，Hub 保留消息顺序并把该消息用于下一个 Turn，不能改投另一个活动 Turn。

## 非目标

- 不由 Hub 模拟 Codex Thread、Turn、Item 或 compaction。
- 不在恢复失败时自动创建替代 Thread，也不从 Hub 历史重放出一个新 Thread。
- 不把停止解释为撤销；已产生的 Workspace、命令、工具和外部副作用全部保留。
- 不允许 Runtime 自行选择、下载或回退 Codex 版本。

## 验收标准

- fake app-server fixture 保留现有 V1 message、usage、tool request/result 和完成事件行为。
- 聚焦协议测试固定 `thread/resume`、`turn/start`、`turn/steer`、`turn/interrupt` 的必需参数、request/response ID 关联和基本终态通知。
- 同一个在线 Session 可在一个 app-server/Thread 上完成多个 Turn，并能 steer 和 interrupt 当前 Turn。
- 长 Turn 期间 Runtime heartbeat 不间断；异常和取消后不存在遗留 app-server 进程。
- active Codex 版本切换不打断当前 Turn；下一 Turn 才使用新版本。
- 删除已绑定 Skill 后，受影响 idle Session 原子移除 Hub-owned 派生文件；active Turn 保持旧文件直到终态，全过程不重启 app-server。
- Agent 默认模型和自定义子 Agent 配置可 materialize 为 Codex 原生配置；全部详细参数通过安装版 Codex 的 `--strict-config` 校验。推理强度遵循“子 Agent > Agent > Model Connection > Codex 自动值”，协议专属请求参数在 Gateway envelope 中随下一 Turn 的连接快照生效，连接变更只在约定的 request/Turn 边界生效。

## 测试计划

- Rust：使用 fake app-server 验证初始化、start/resume、start/steer/interrupt、响应 ID、事件映射、失败不 fallback、超时和进程回收。
- Frontend：构建通过。
- 浏览器：Session 继续对话、活动 Turn 引导和显式停止链路通过。
