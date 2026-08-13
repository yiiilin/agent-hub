# 会话恢复与升级无感化机制

> 领域文档：runtime 升级/重启/异常后，AI 会话如何自动恢复，用户无感。

## 1. 问题背景

AI 会话状态分布两处，异常时无法对齐：

| 数据源 | 内容 | 保留策略 |
|---|---|---|
| 引擎本地（jsonl + workspace） | 消息/推理轨迹/工作区文件 | 随 runtime 卷，异常时访问权可能丢失 |
| hub run_events | 完整事件流（user/assistant/tool） | 永久无裁剪（仅注销/删会话清理） |

历史缺陷：释放后 bundle checkpoint 停旧 → unreplayable 检查永久阻塞领取（run 永远 pending）；引擎空启动产生新 native Session id 与 Hub 旧绑定冲突（turn_started 409，会话进入 recovery_failed）；reap 阈值 30s < runtime HTTP 超时 60s（一次慢请求误杀会话）。

## 2. 权威链（恢复的数据基础）

| 数据源 | 用途 |
|---|---|
| run_events | 对话与工具调用历史的**唯一权威源**（assistant 回复不在消息表，只在事件表） |
| hub_session_messages | 待办识别（queued/delivering = 未处理） |
| Bundle | **仅工作区文件快照**（不再含 Pi 会话/engine-state，不做 native Session 校验） |

## 3. 恢复语义（全自动，用户无感）

```
runtime 停止/drain（SIGTERM/SIGINT）：
  ① 立即杀所有 pi 进程（不等推理）
  ② 逐会话打包（仅 workspace 快照 + manifest）→ 上传 hub（总时限 100s）
  ③ hub 记录 bundle_sync_status（pending/uploading/done/failed），管理端可见剩余数量
  ④ 兜底强制释放所有权

runtime 启动 → claim 会话：
  ├─ 需要恢复（跨 runtime 接管 lifecycle=restoring，或 canonical Pi 会话文件缺失）
  │    → GET replay-events 从 run_events 拉对话/工具事件
  │    → 重建 Pi 会话 jsonl（首行 id = canonical native Session id）
  │    → 引擎加载重建 jsonl（历史重现，turn_started 绑定一致 → 无 409）
  │    → 有 Bundle 时仅恢复 workspace 文件快照
  └─ 正常续聊（online 且本地 jsonl 存在）→ 本地 jsonl 继续，不重建
```

关键点：
- **重建失败直接传播**（restoring 场景由 Hub 转 recovery_failed），不得空启动/回退本地文件——空启动正是 409 的来源。
- 重建 jsonl 的首行 session id 必须是 Hub 记录的 canonical native Session id（数据库触发器禁止替换绑定），因此引擎恢复后 turn_started 天然一致。
- 工具历史分类关联：内置工具（bash/read 等，item dynamicToolCall/commandExecution completed）按 (run_id, item_id) 关联；integration 工具（run_shelves_operation 等）按全局 tool_request_id 跨父/子 Run 关联（tool_request 在父 Run，client_tool_result 在 follow-up Run）。
- 无结果调用不写入重建 jsonl（等待审批的 waiting_tool 由 Hub 状态与 tool_result continuation 接管，不伪造可继续语义）。

## 4. 重建映射（run_events → Pi jsonl）

| 事件 | 重建行为 |
|---|---|
| message(user/assistant) | 对应消息行（content=[text]） |
| model_request | 模型调用边界：结束上一个 pending 输出段 |
| item dynamicToolCall/commandExecution completed | toolCall 行 + toolResult 行（同一 assistant 输出段内合并 text+toolCall） |
| tool_request（integration） | toolCall 行（有对应结果时，id=Hub tool_request_id） |
| client_tool_result / tool_result | toolResult 行（按 tool_request_id 跨 Run 关联，client 优先去重） |
| usage / message.stop_reason | assistant 行 usage/stopReason 元数据（usage 双键兼容 input/output 与 input_tokens/output_tokens） |
| reasoning/status/turn 边界等 | 跳过 |

## 5. 关键修复（git log）

| 提交 | 内容 |
|---|---|
| 3a33511 | force release/reap 释放清空 bundle 归档（消除 unreplayable 永久阻塞） |
| 2f0159c | reap 阈值 30→90s（> HTTP 超时 60s）；bundle_sync 字段/状态 API；patch-messages 端点 |
| 85fc2a6 | runtime：SIGTERM 杀 pi + 逐会话打包上传；恢复历史补丁注入（含自动继续） |
| c479d45 | PUT bundle-sync 状态更新端点 |
| d3d6bf4 | compose stop_grace_period 120s；管理端打包进度；部署文档 |
| （本次） | Bundle 收窄为 workspace-only（create/restore/checkpoint/salvage 全链路去 native-session）；replay-events 端点 + 重建 Pi jsonl（DB 事件唯一事实源）；补丁注入退役；claim 终结残留 turn + 清理 active_turn 指针；eligibility 恢复"仅 waiting_tool+pending request 阻断"；heartbeat 保护 restoring；独立 keepalive 循环（10s，轻量端点，不释放空闲会话） |

## 6. 升级操作（用户视角）

```bash
docker compose pull
docker compose up -d --force-recreate
```

预期：旧 runtime SIGTERM → 100s 内逐会话打包上传（stop_grace_period 120s 保证）→ 新 runtime 恢复（Bundle 恢复 workspace + DB 事件重建会话历史）→ 会话自动继续（面板显示"AI 连接恢复中…"）。最坏情况（打包超时/跨机）：工作区文件丢失，对话历史由 DB 事件完整重现。

## 7. 运维可见性

- 管理端：系统参数页"Bundle 打包进度"（每 runtime：total/pending/uploading/done/failed，剩余= pending+uploading 红色徽章）
- 面板：恢复期间显示"AI 连接恢复中…"提示条
