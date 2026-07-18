# Hub-Managed Skills 功能链 Spec

## 范围

1. 登录用户可创建、查看、更新和物理删除自己的 Hub-managed Skill；不存在归档状态。
2. Skill 包含 `name`、`description`、Markdown `content`、revision 和内容 checksum，由 Hub 保存并按 owner 隔离。
3. Agent 可绑定多个自己有权使用的 managed Skill；Agent-inline Skill 和 `skills_manifest` 不再存在。
4. 开始新 Turn 前，Runtime 使用包含全部有效 Skill revision/checksum 的确定性 fingerprint 同步 Session 专属 Codex 目录。
5. 活动 Turn 期间不改写 Agent/Skill 文件；Steering Messages 使用 Turn 开始时的稳定文件集。
6. 删除 Skill 在一个事务中删除 Skill 和全部 Agent 绑定，并为受影响的在线 Session 请求最新配置刷新。
7. 空闲 Session 立即刷新；活动 Turn 在终态后、下一 Turn 前刷新。刷新不重启 app-server，不改 Workspace。
8. 离线 Session 不保留派生 Skill 文件，下次恢复后只 materialize 当前仍有效的 Skills。
9. 管理台使用统一 Markdown 所见即所得编辑器；Skill 列表提供复选框、当页全选和批量删除。

## 非目标

- 不实现 Skill 归档/恢复、Agent-inline Skill、公开市场、跨用户共享或审批流。
- 不把 Runtime 本地 Skill 自动探测为 Hub 配置来源。
- 不强制向 Turn 注入 Skill 全文；Codex 是否重读由 native 行为决定。

## 验收标准

- `/skills` 可创建、编辑、单个删除和批量删除 Skill，删除后 API 与列表不再返回。
- 批量删除为 owner 范围内全有或全无；包含他人或不存在 ID 时返回 404 且不删除任何项。
- Agent 页默认展示已启用 Skill，通过单独管理按钮打开选择子菜单。
- 删除被多个 Agent 绑定的 Skill 后，所有绑定消失，受影响的在线 Session 中 Hub-owned 派生文件被清理。
- 活动 Turn 中删除 Skill 不改变该 Turn；终态后刷新且不重启 app-server。

## 测试计划

- Rust：覆盖 owner 隔离、revision/checksum、单个/批量删除事务、Agent 自动解绑、refresh command fencing 和活动 Turn 延迟。
- Runtime：覆盖空闲立即刷新、Turn 终态刷新、跨 Session 隔离、原子 materialization 和 Workspace 不受影响。
- 浏览器：覆盖 Markdown 编辑、Agent 绑定、列表复选和批量删除。
