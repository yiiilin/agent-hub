# Agent 管理功能链 Spec

## 范围

1. Agent 支持更新 name、Markdown instructions、visibility、`public_to`、Runtime 约束、managed Skills 和 MCP allowlist。
2. `private`、`public_to` 和 `public` 仍是完整后端权限模型；只有 `admin` 和 `super_admin` 可创建或把 Agent 改为 `public`。
3. sandbox policy 继续持久化并参与 Runtime 执行但不在管理台展示；模型改用 `docs/model-connections-spec.md` 的独立 Model Connection、Agent default、reasoning effort 和 Codex Subagent Definition 表单。
4. Agent 只绑定 Hub-managed Skills，不提供 `skills_manifest` 或 inline Skill。页面默认展示已启用 Skill，选择放在独立子菜单。
5. MCP 保留结构化 allowlist 和 secret 脱敏，页面以表格展示，新建/编辑使用子表单，删除保留确认。
6. Integration App 不再放在 Agent 配置 Tab 中，而是独立一级导航；Agent 页只可展示与其有关的读取摘要或跳转入口。
7. Agent 删除不可逆；取消未完成 Run，删除可执行配置、关联和运行数据，保留最小 Agent 展示快照与 Historical Sessions 只读历史。
8. Session 永久绑定创建时选择的 Agent；切换 Agent 必须新建 Session。

## 非目标

- 不实现隐式 MCP server、inline Skill、Agent 归档/恢复或从 Historical Session 继续对话。
- 不因 Agent/Skill 更新强制重启 app-server 或向模型注入文件全文。
- 不在前端暴露 sandbox policy、provider secret 或任意 provider headers；模型选择只引用权限范围内的 Model Connection。

## 验收标准

- Agent instructions 使用统一所见即所得/Markdown 源码编辑器，新建和编辑默认只显示操作按钮。
- Agent 可选择 owner 范围内的默认 Model Connection 和 reasoning effort，并通过子表单维护不扩大 Workspace、Skill、MCP 或 sandbox 权限的 Codex Subagent Definitions。
- member 无法在 UI 或 API 中创建 `public` Agent；admin/super_admin 可以。
- 管理台不出现 raw model policy JSON、sandbox policy 或 inline Skill 编辑器；模型只通过结构化 Model Connection 和 reasoning controls 配置。
- Skill 选择器默认收起，已启用 Skill 在主页直接可见。
- MCP 表格中 secret 始终脱敏，保存脱敏占位符不覆盖旧值。
- 删除 Agent 后历史 Session 仍可查看，所有新消息、Run 或 Turn 入口被拒绝。

## 测试计划

- Rust：覆盖 DTO、visibility 角色限制、MCP 脱敏、Runtime 配置 materialize、删除并发和历史只读。
- TypeScript：前端构建通过。
- 浏览器：覆盖 Agent 新建/编辑、Markdown、Skill 选择、MCP 表格、角色权限、删除和历史查看。
