# Automations 功能链 Spec

## 范围

本阶段实现 plan.md 中 Automations 的最小完整闭环：

1. 登录用户可以为自己的 Agent，或当前用户可调用的公共（`public` / `public_to`）Agent 创建 Automation；自动化归创建者所有，触发时以 Agent 自身配置运行。
2. Automation 支持 `manual`、`webhook`、`interval`、`cron` 四种 trigger 类型的配置落库。
3. `manual` trigger 通过管理台按钮启动指定 Agent run。
4. `webhook` trigger 通过 scoped token 启动指定 Agent run。
5. 触发后复用现有 pending -> runtime claim -> events -> completed 链路。
6. 管理台以 Automation 列表为页面主体，展示状态、trigger 类型、webhook URL 和最近触发时间。
7. owner 可以通过 `PATCH /api/automations/{automation_id}` 编辑 `name`、`trigger_type`、`prompt`、`schedule` 和 `enabled`；`agent_id` 不可更换。
8. 新建和编辑默认只显示操作按钮，点击后在独立抽屉表单中操作；关闭表单返回原列表上下文。
9. Automation prompt 使用项目统一的 Markdown 所见即所得/源码编辑器，API 仍存储普通 Markdown 字符串。
10. 内置 scheduler 已按 `docs/automation-scheduler-spec.md` 实现；编辑 `trigger_type`、`schedule` 或 `enabled` 后，后续扫描使用更新后的持久化配置。
11. Automation 触发的 run 通过 nullable `runs.automation_id` 记录来源；删除 Automation 时 run 保留且关联置空。
12. owner 可以通过 `GET /api/automations/{automation_id}/runs?page=&page_size=` 分页查看精确 Automation 的执行历史。页码从 1 开始，默认每页 20 条、最多 100 条，按 `created_at DESC, id DESC` 稳定排序。
13. 管理台在 Automation 详情中展示当前页运行历史，并可选择 run 复用完整 Run Console 事件视图。

## 非目标

- 不实现复杂模板语言；触发消息只使用请求 message 或 Automation 默认 prompt。
- 不实现跨 owner 调用；Automation 始终以 Agent owner 身份创建 run。

## 验收标准

- 管理台 `/automations` 可以创建 Automation。
- 列表始终是默认主视图；新建/编辑抽屉不把页面替换为常驻表单。
- prompt 可在所见即所得和 Markdown 源码间切换并无损保存。
- 点击 manual trigger 后，Automation 对应 Agent 出现新 run，并由 runtime 完成。
- webhook URL 在未登录情况下可以用 token 触发 run。
- disabled Automation 不能触发。
- Automation 不能为不属于当前用户的 private Agent 创建；public / public_to 且未归档的 Agent 可创建。
- Agent 对 Automation owner 失去可见性（如撤销 public）后，该 Automation 的新创建、编辑与触发均被拒绝；owner 本人不受影响。
- owner 更新返回当前持久化状态；foreign 或不存在统一返回 404，归档后的 Agent 不接受更新。
- 非 webhook 转为 webhook 时生成一次性明文 token；webhook 保持 webhook 时保留原 hash 且不再次返回 token；转出 webhook 时清除 hash。
- Automation 列表和后续读取永不包含明文 webhook token。
- 创建、更新、归档、手动触发、webhook 触发与 scheduler 遵循先锁 Agent、再锁 Automation 的顺序。
- manual、webhook、interval 和 cron 触发创建的 run 均关联精确 Automation；Agent console、widget 和 integration run 的 `automation_id` 为 `null`。
- Automation 历史只对 owner 开放，foreign 与不存在统一返回 404；非法分页返回 400。
- 历史列表显示 status、source、initial message、created 和 updated；仅当前页存在 `pending`、`running` 或 `waiting_tool` 时串行轮询。
- 切换 Automation 时重置页码和已选 run；触发后刷新第一页。历史与事件加载均提供 loading、error/retry 和 empty 状态。
- failed reason 仅来自 Run Console 事件，不在 runs 表重复存储。

## 测试计划

- Rust：覆盖 owner 更新、foreign 404、输入校验、webhook token 转换与归档竞争；后端构建和现有测试通过。
- TypeScript：前端构建通过。
- 浏览器：Playwright 覆盖列表主视图、新建/编辑抽屉、Markdown prompt、manual/webhook trigger、history 到 Run Console、分页、轮询竞态和 390px 布局。
- 手工 API：验证 webhook token 可触发，禁用或无效 token 被拒绝。
