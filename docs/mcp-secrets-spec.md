# MCP Secret 注入功能链 Spec

## 范围

1. Agent MCP allowlist 继续使用显式 JSON 数组，空数组表示无 MCP，但管理台不再暴露原始 JSON 编辑器。
2. 管理台以表格显示 MCP entries，通过新建、编辑子表单和带确认的删除操作维护原数组。
3. MCP entry 可包含 `secrets` 对象；Hub 在控制面 API 和 UI 中返回脱敏值。
4. 用户保存从 UI 读回的脱敏占位符时不覆盖原 secret，新 secret 只在表单提交中短暂存在。
5. Pi Runtime V1 不实现 MCP 执行，因此不把 MCP allowlist、命令或 secret 物化到 Session 的 `engine-state/`。
6. Session Bundle、Workspace、Execution Engine state、日志、argv 和浏览器快照不得包含 MCP secret。

## 非目标

- 不接入外部 secret manager，不实现字段级权限模型。
- 不实现 Pi MCP 执行、MCP server 启动或 Runtime-side secret 注入。

## 验收标准

- Agent 详情中 MCP 以 table 展示，只有点击新建/编辑后才出现子表单。
- `GET /api/agents/{id}` 和列表不返回 MCP secret 明文；脱敏占位符 round-trip 保留原 secret。
- Runtime Session 中不存在 `mcp-allowlist.json`，`engine-state/` 和其他 Session 文件均不含 MCP secret 或 Subagent 定义。

## 测试计划

- Rust：覆盖 MCP secret 脱敏、占位符 merge 和 Pi Runtime 不物化 MCP。
- TypeScript：前端构建通过。
- 浏览器/API QA：覆盖 MCP 表格新建/编辑/删除、secret 脱敏与运行后无落盘断言。
