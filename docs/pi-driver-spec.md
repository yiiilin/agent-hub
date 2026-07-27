# Pi Runtime Driver

## 目标

Pi 是 Agent Hub Runtime 当前的原生执行进程。Hub 仍拥有 Session、Run、
Workspace、权限、模型连接、provider key、用量账本和 Bundle 生命周期；Pi
只在一个在线 Hub Session 内执行一条可恢复的原生会话。

本迭代不实现 MCP 或 Pi Subagent。Hub 的公开 DTO 和数据库使用
`engine_version` 表示 Runtime Engine 版本，使用 `native_session_id` 表示 Pi
Native Session ID；Bundle 的 `native-session/` 子树保存 Pi JSONL recovery data。
Pi Runtime 注册时上报 `mcp_allowlist: false` 和 `subagents: false`；Hub 不会将
含已启用 MCP 或 Subagent 配置的 Run 领取给该 Runtime，而是以 capability mismatch
结束该 Run，避免静默忽略配置。

## 进程和目录

每个在线 Session 恰有一个 Pi RPC 子进程：

```text
session-root/
  workspace/                 # Pi cwd，跨 Turn 保留
  engine-state/              # Pi 的隔离 HOME
    .pi/agent/AGENTS.md      # Hub 生成的 Agent 指令
    .pi/agent/models.json    # 只指向 Runtime loopback proxy
    .pi/agent/skills/        # 当前 Session 的 Hub/本地 Skill 快照
    sessions/                # Pi JSONL native session state
    skill-exec/              # 当前 Session 的 Package 执行副本、catalog 和临时目录
  supervisor/session.json    # Hub owner/generation/idle metadata
  staging/                   # 不归档的临时文件
```

Runtime 用以下形状启动 Pi：

```text
HOME=<session-root>/engine-state
pi --mode rpc --session-dir <session-root>/engine-state/sessions [--session <saved-jsonl>]
```

Runtime 明确传递 Workspace cwd、进程组、受限环境变量和工具 allowlist。Pi
不读取 Runtime 用户的真实 home，也不共享另一个 Hub Session 的 Pi agent
目录、JSONL 文件或 Workspace。

## RPC 映射

| Hub 行为 | Pi RPC | Runtime 处理 |
| --- | --- | --- |
| 新 Session / Bundle 恢复 | `get_state` | 保存 Pi Native Session ID 和 `sessionFile` |
| 设置主模型 | `set_model` | provider 仅是当前 Run binding 的本地代理 |
| 设置推理强度 | `set_thinking_level` | Pi `max` 可通过 model map 映射到 upstream `ultra` |
| 原子配置刷新 | `reload_resources` | 重新发现 AGENTS.md 和 Skills，不重启进程或 Native Session |
| 空闲用户消息 | `prompt` | Hub 已持久化的有序普通消息合并为一次 prompt |
| active Turn 的立即引导 | `steer` | 只针对当前 Hub Turn；旧 Turn 被拒绝时回到下一 Turn |
| 显式停止 | `abort` | 不撤销已写文件、已执行命令或已持久化事件 |
| `turn_start` | Pi event | 解锁当前 Run 的 loopback model proxy |
| `message_update` | Pi event | 文本和 thinking 以既有 Run event 契约流式写入 |
| `tool_execution_*` | Pi event | 映射为命令/工具活动，保留增量输出 |
| `turn_end` / `agent_settled` | Pi event | 只在 settled 后结束本次 Hub Run |

JSONL 只接受 LF 分隔的完整 JSON 记录。未知、畸形或与当前请求不匹配的
response/event 是 Runtime driver error，子进程会被停止并由现有 Run 失败路径
处理。

## 模型代理和设置

Pi `models.json` 为每个 Run 的 `main` binding 创建一个 provider：

- `api` 固定为 `openai-responses`，`baseUrl` 是 Runtime loopback `/v1`。
- `x-agent-hub-model-binding-id` 是唯一可交给 Pi 的路由信息。
- Pi 不得到真实 provider URL、provider API key、Hub model-proxy token 或
  S3 credential。
- Hub 继续根据 binding 的上游协议处理 Responses 透传或协议改写，并记录
  成功 response 内的 usage 和失败 response 的错误。
- `context_window_tokens`、可表示的 output limit 和 reasoning map 由 Pi model
  config 使用；`temperature`、`top_p` 和 protocol-specific output settings
  仍由 Hub gateway 根据 binding 合并，不能在 Pi 侧重写。

## 工具策略

Pi standalone 没有统一的 sandbox policy RPC 参数。Hub 先从 Agent 的显式
tool allowlist 开始；Integration App 配置存在时取交集。公开 Widget 再限制为
`read,grep,find,ls`，并把 sandbox 改为 read-only、关闭网络和 MCP。Runtime 最后
按下表与 sandbox policy 再取更严格结果，不得把 Hub policy 静默放宽：

| Hub sandbox policy | `network_access` | Pi builtin tools |
| --- | --- | --- |
| `read-only` | 任意 | `read,grep,find,ls` |
| `workspace-write` | `false` | `read,grep,find,ls,edit,write` |
| `workspace-write` | `true` | 上述工具加 `bash` |
| `danger-full-access` | `false` | `read,grep,find,ls,edit,write` |
| `danger-full-access` | `true` | 上述工具加 `bash` |

`integration` 不是 Pi builtin 名称。只有有效策略仍包含它时，Runtime 才把当前
Integration Session 声明的具体工具名加入 Pi allowlist；没有 Integration context
时不会凭该标志生成工具。Agent/App/public/sandbox 任一层拒绝的工具都不能被后续层
重新加入。

`skill_exec` 也是 Runtime 注册的 Pi Extension 工具，不是 Pi builtin 或 `bash` 的
别名。启用 Skill 不会自动补回 `read`；只有最终 execution configuration 允许
`skill_exec`，且当前 Session 至少一个已启用 Package 的 manifest 声明了 `bin/*`
可执行文件时，Runtime 才注册该工具。调用只接受 catalog 中精确的 Skill 名和
`bin/` 相对路径，不经过 shell。脚本 shebang 只接受一个受控解释器：直接解释器
路径不得带参数，`/usr/bin/env` 后也只能有一个解释器名。Runtime 为每次调用创建
私有临时 HOME，以 Linux Landlock 将 Package 设为只读，并按最终文件工具权限限制
Workspace；若最终策略没有任何可读 Workspace 的文件工具，`skill_exec` 也不能读取
Workspace。参数、stdin、stdout/stderr、超时和子进程组均有硬限制；主程序正常退出
后也会终止其遗留后台进程。非 Linux Runtime 不提供 `skill_exec`。

Pi 的 `--tools` 和 Linux Landlock 文件权限都在进程启动时固定。Runtime 记录在线
Pi 进程的有效工具集合。配置刷新不打断活动 Turn；Runtime 在配置文件原子切换后
调用 Pi 原生 `reload_resources`，使原进程重新发现 AGENTS.md 和 Skills。下一 Turn
的有效工具集合发生变化时，Runtime 必须先停止空闲 Pi，再从同一 JSONL 恢复同一个
Native Session 并传入新的 `--tools`；集合未变化时不重启进程。

Pi 进程仍运行在 Runtime 的非 root 用户、容器/操作系统隔离和每个 Session
Workspace 下。此表不承诺其他执行引擎 sandbox 的字节级等价；若部署要求比 Runtime
容器更强的 host-level shell/network 隔离，必须在 Runtime 节点部署边界提供，
而不是由 Hub UI 或模型提示词假装实现。

## Bundle

Bundle 只包含 `workspace/`、`manifest.json` 和 `native-session/`。最后一个目录
只允许 `sessions/` 及一个直接位于其下、首行 `type=session` 且 `id` 与 manifest
中的 `native_session_id` 相同的 Pi recovery JSONL。
创建和恢复都不以文件名判断 Session 身份。不得包含：

- `.pi/agent/models.json`、`auth.json`、settings、extensions、Skills、`skill-exec/` 或缓存；
- Hub Runtime credential、model-proxy token、provider key、S3 credential；
- Pi binary、日志、npm/Bun 目录、session index/metadata 或其他 Session 文件。

恢复后 Hub 在 Pi 启动前重新物化 Agent instructions、Skills 和当前 model
binding。Pi 是否在新 Turn 使用新增 Skill 内容由其原生资源加载机制决定。

## Fixture Contract

`deploy/fake-pi-rpc.sh` 是确定性开发/E2E fixture，不是 Pi 的替代实现。它：

- 只接受 `--mode rpc`，并实现 `get_state`、`set_model`、
  `set_thinking_level`、`prompt`、`steer` 和 `abort`；
- 发出 Pi 风格的 `agent_start`、`turn_start`、`message_update`、
  `tool_execution_*`、`turn_end`、`agent_end` 和 `agent_settled`；
- 使用 `FAKE_PI_DISABLE_MODEL=1` 时不发出网络请求，适合纯协议测试；
- 正常模式从隔离 Pi `models.json` 读取 loopback URL 和 binding header，以
  fake provider 跑完整 Runtime -> Hub gateway 链路；
- 不接受不属于 Pi standalone RPC 的 JSON-RPC `initialize`、`thread/start` 或
  `turn/start`，防止 fixture 掩盖 driver 协议错误。
