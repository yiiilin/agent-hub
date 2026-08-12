# 会话恢复与升级无感化机制

> 领域文档：runtime 升级/重启/异常后，AI 会话如何自动恢复，用户无感。

## 1. 问题背景

AI 会话状态分布两处，异常时无法对齐：

| 数据源 | 内容 | 保留策略 |
|---|---|---|
| 引擎本地（jsonl + workspace） | 消息/推理轨迹/工作区文件 | 随 runtime 卷，异常时访问权可能丢失 |
| hub run_events | 完整事件流（user/assistant/tool） | 永久无裁剪（仅注销/删会话清理） |

历史缺陷：释放后 bundle checkpoint 停旧 → unreplayable 检查永久阻塞领取（run 永远 pending）；bundle 缺失时引擎只注入当轮消息（AI 失忆）；reap 阈值 30s < runtime HTTP 超时 60s（一次慢请求误杀会话）。

## 2. 权威链（恢复的数据基础）

| 数据源 | 用途 |
|---|---|
| run_events | 对话还原/补丁注入的**唯一权威源**（assistant 回复不在消息表，只在事件表） |
| hub_session_messages | 待办识别（queued/delivering = 未处理） |
| 引擎 jsonl | bundle 深度恢复（有则用；可能滞后写盘） |

## 3. 恢复 = 二选一（全自动，用户无感）

```
runtime 停止/drain（SIGTERM/SIGINT）：
  ① 立即杀所有 pi 进程（不等推理）
  ② 逐会话打包（workspace + jsonl）→ 上传 hub（总时限 100s）
  ③ hub 记录 bundle_sync_status（pending/uploading/done/failed），管理端可见剩余数量
  ④ 兜底强制释放所有权

runtime 启动 → claim 会话：
  ├─ 本地 jsonl 存在 → 引擎本地恢复（记忆+文件，不补丁）
  ├─ 无本地 jsonl + bundle 存在 → 解压恢复 + 补丁注入
  └─ 无 bundle → 引擎空启动 + 全量补丁注入（对话永不丢）
```

## 4. 补丁注入语义

恢复场景（引擎空启动）时，从 run_events 拉全量 user/assistant 消息分类注入：

- **已闭环**（user 后有 assistant 回复）→ 历史段："已处理的历史对话，请知晓上下文，不要重复回复"
- **未闭环**（最后 user 无回复）→ 待办段："恢复前未及回复的请求，请现在处理"（= 恢复后自动继续）

有本地 jsonl 时引擎自恢复、不补丁（避免重复）；注入走 `claim.session_context.messages` 前缀（引擎 prompt 构造天然包含）。

## 5. 关键修复（git log）

| 提交 | 内容 |
|---|---|
| 3a33511 | force release/reap 释放清空 bundle 归档（消除 unreplayable 永久阻塞） |
| 2f0159c | reap 阈值 30→90s（> HTTP 超时 60s）；bundle_sync 字段/状态 API；patch-messages 端点 |
| 85fc2a6 | runtime：SIGTERM 杀 pi + 逐会话打包上传；恢复历史补丁注入（含自动继续） |
| c479d45 | PUT bundle-sync 状态更新端点 |
| d3d6bf4 | compose stop_grace_period 120s；管理端打包进度；部署文档 |

## 6. 升级操作（用户视角）

```bash
docker compose pull
docker compose up -d --force-recreate
```

预期：旧 runtime SIGTERM → 100s 内逐会话打包上传（stop_grace_period 120s 保证）→ 新 runtime 恢复 → 会话自动继续（面板显示"AI 连接恢复中…"）。最坏情况（打包超时/跨机）：只丢环境文件，对话永远在（消息补丁）。

## 7. 运维可见性

- 管理端：系统参数页"Bundle 打包进度"（每 runtime：total/pending/uploading/done/failed，剩余= pending+uploading 红色徽章）
- 面板：恢复期间显示"AI 连接恢复中…"提示条
