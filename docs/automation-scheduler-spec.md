# Automation Scheduler 功能链 Spec

## 范围

本阶段补齐 `plan.md` 中 cron/interval 自动化的最小可验收闭环：

1. 后端启动一个内置 scheduler loop，定期扫描启用的 `interval` 和 `cron` automation。
2. `interval` schedule 支持 `Ns`、`Nm`、`Nh`，例如 `2s`、`5m`、`1h`。
3. `cron` schedule 支持 UTC 5 字段基础格式；字段可为 `*` 或单个数字，按分钟触发，weekday 使用 `0` 或 `7` 表示 Sunday。
4. 自动触发复用现有 `create_run_for_agent`，run source 为 `automation:scheduler`。
5. 触发成功后更新 `last_triggered_at`，UI 通过刷新 automation 列表展示最近运行时间。

## 非目标

- 不实现复杂 cron 表达式：范围、列表、步长、时区配置暂不支持。
- 不新增分布式调度表或 job queue；V1 Docker Compose 只有一个 backend scheduler。
- 不新增独立暂停接口；通过 Automation PATCH 的 `enabled` 字段暂停或恢复。

## 验收标准

- 创建 `interval` automation 时必须有合法 schedule；非法 schedule 返回 400。
- 启用的 `interval` automation 到期后自动创建 run，run 完成后列表显示 `Last run`。
- 启用的 `cron` automation 在匹配分钟内最多触发一次。
- 禁用 automation 不会被 scheduler 触发。
- 浏览器测试覆盖创建 2s interval automation 并观察自动运行完成。

## 测试计划

- Rust：覆盖 interval schedule 解析、cron 基础匹配、同一分钟去重、禁用跳过。
- TypeScript：前端构建通过。
- 浏览器：Playwright 创建 interval automation，等待 scheduler 自动创建 run 并完成。
