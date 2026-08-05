# Hub-Managed Skills 功能链 Spec

## 范围

1. 登录用户可创建、查看、更新和物理删除自己的 Hub-managed Skill；不存在归档状态。
2. Skill 包含 `name`、`description`、Markdown `content`、revision 和内容 checksum，由 Hub 保存并按 owner 隔离；还可附加一个由普通文件组成的当前 Package。
3. Agent 可绑定多个自己有权使用的 managed Skill；Agent-inline Skill 和 `skills_manifest` 不再存在。
4. 管理台可以选择多个文件或一个目录上传。根目录必须有 UTF-8 `SKILL.md`，其中 YAML frontmatter 的 `name`、可选 `description` 和正文会原子替换 Skill 元数据；其余文件作为 Package 保存，全部标记为可执行（运行时白名单为除 `SKILL.md` 外的任意文件）。
5. 开始新 Turn 前，Runtime 使用包含全部有效 Skill revision/checksum 和 Package 元数据的确定性 fingerprint，同步 Session 专属 `engine-state/.pi/agent/skills/` 与私有执行副本。
6. 活动 Turn 期间不改写 Agent/Skill 文件；Steering Messages 使用 Turn 开始时的稳定文件集。
7. 删除 Skill 在一个事务中删除 Skill 和全部 Agent 绑定，并为受影响的在线 Session 请求最新配置刷新。
8. 空闲 Session 立即刷新；活动 Turn 在终态后、下一 Turn 前刷新。刷新不改 Workspace；仅当下一 Turn 的有效工具集合变化时才替换空闲 Pi 进程，并恢复同一个 Native Session。
9. 离线 Session 不保留派生 Skill 文件，下次恢复后只 materialize 当前仍有效的 Skills。
10. 管理台使用统一 Markdown 所见即所得编辑器；Skill 列表提供复选框、当页全选和批量删除。

## Package 契约

- `PUT /api/skills/{skill_id}/package` 使用有序 multipart manifest。只接受 1 到 1024 个唯一、安全、相对路径的普通文件；拒绝绝对路径、反斜杠、空组件、`.`、`..`、NUL、重复项及文件/目录路径重叠。
- 展开后总大小上限为 512 MiB，Hub 生成的 `tar.zst` 上限为 256 MiB。Hub 为归档及每个文件记录 SHA-256；Package format version 固定为 `1`。
- 替换 Package、更新由 `SKILL.md` 导出的内容、递增 revision、发布 Agent 配置刷新和切换 current Package 在同一数据库事务中完成。新对象写入失败不改变 current Package；旧对象通过可重试删除队列清理。若事务提交结果不确定，新对象也只进入删除队列；worker 确认它既不是 current Package、也没有被活动 Run 快照引用后才可删除。
- 每个 Run 在领取事务中快照其 Package ID、文件 manifest、大小、checksum 和对象键。活动 Run 始终下载该快照；Turn 间配置刷新下载 Session 当前 Package，二者都校验 Runtime credential 与 `ownership_generation`。
- Hub 可用 `HUB_SKILL_PACKAGE_STORAGE=local|s3` 保存 Package。Runtime 不接触对象存储凭据或对象 URL，只经 Hub 流式下载。

## Session 物化与执行

```text
RUNTIME_WORK_ROOT/
  skill-package-cache/<archive-sha256>.tar.zst       # Runtime 共享只读压缩缓存
  sessions/<session-id>/engine-state/
    .pi/agent/skills/<slug>/                         # Pi 原生 Skill 读取副本
    skill-exec/catalog.json
    skill-exec/tmp/                                  # 每次调用独立临时目录
```

- Skill 只有一份物化副本（`.pi/agent/skills/<slug>/`）：上传归档排除根 `SKILL.md`（内容存为 Skill 正文，物化时重新生成），其余文件全部标记为可执行并进入 `skill-exec/catalog.json` 白名单；目录与文件权限为 `0550/0440`，对 Pi 只读。压缩缓存只按 SHA-256 复用并在每次命中时校验大小/checksum；解包目录绝不跨 Session 共享，完整 manifest、tar entry 类型、路径、大小、checksum 和 executable 标志均由 Runtime 再验证。
- Pi 通过原生 Skill discovery 发现 `.pi/agent/skills/<slug>/SKILL.md`，是否读取正文由 Pi 自身决定。启用 Skill 不会自动授予 `read`。
- `skill_exec` 是显式 Agent/App 工具权限，不是通用 shell。它仅在最终工具交集仍允许 `skill_exec` 且至少一个已启用 Package 含可执行文件（除 `SKILL.md` 外的任意文件）时注册；请求必须精确匹配当前 Session catalog 中的 Skill 名和程序路径。
- `skill_exec` 不拼接 shell 命令。脚本只能使用无参数的受控 shebang；每次调用使用独立 `HOME`/`TMPDIR`，Package 只读。是否可读写当前 Workspace 继续服从该 Agent 的最终文件工具权限，仅授权 `skill_exec` 时不能读取 Workspace。Linux Runtime 用 Landlock、私有 loopback TCP broker/随机 token、参数/输入/输出上限、超时和进程组终止约束子进程；主程序退出后也终止遗留后台进程。非 Linux Runtime 不开放该工具。
- Session 配置原子切换成功后，Runtime 调用 Pi 原生 `reload_resources` 重新发现 Skill；活动 Turn 延迟到终态后处理。工具集合不变时不重启 Pi 或 Native Session，工具集合变化时在下一 Turn 前从原 JSONL 恢复同一个 Native Session。
- Session Bundle 只保存 Workspace 和 Pi recovery JSONL，不保存 Package 派生副本、catalog、临时目录或 Runtime 压缩缓存。

## 非目标

- 不实现 Skill 归档/恢复、Agent-inline Skill、公开市场、跨用户共享或审批流。
- 不把 Runtime 本地 Skill 自动探测为 Hub 配置来源。
- 不强制向 Turn 注入 Skill 全文；Pi 是否重读由其原生行为决定。
- 不允许 Package symlink、device、socket、FIFO 或任意目录外程序，也不把 `skill_exec` 扩展为通用命令执行器。

## 验收标准

- `/skills` 可创建、编辑、单个删除和批量删除 Skill，删除后 API 与列表不再返回。
- 批量删除为 owner 范围内全有或全无；包含他人或不存在 ID 时返回 404 且不删除任何项。
- Agent 页默认展示已启用 Skill，通过单独管理按钮打开选择子菜单。
- 删除被多个 Agent 绑定的 Skill 后，所有绑定消失，受影响的在线 Session 中 Hub-owned 派生文件被清理。
- 活动 Turn 中删除 Skill 不改变该 Turn；终态后刷新且不重启 Pi。
- 上传/替换/移除 Package 后，API 返回当前文件清单；失败替换保留旧 Package，Run 快照不被后续替换改变。
- 两个 Session 即使启用同一 Package，也只有压缩缓存可共享；Pi Skill/可执行目录（`.pi/agent/skills`）、catalog 和临时文件均位于各自 Session。
- 未授权 `read` 时 Skill 不补回 `read`；未授权 `skill_exec` 或 Package 没有文件时 Pi 工具列表不含 `skill_exec`。

## 测试计划

- Rust：覆盖 owner 隔离、上传路径/大小/checksum、替换回滚、Run 快照、单个/批量删除事务、Agent 自动解绑、refresh command fencing 和活动 Turn 延迟。
- Runtime：覆盖下载认证与 generation、缓存损坏恢复、安全解包、原子 materialization、跨 Session 隔离、Bundle 排除，以及 `skill_exec` catalog、Landlock、临时目录、限额、超时和进程组清理。
- 浏览器：覆盖 Markdown 编辑、文件/目录上传、Package 文件清单、替换/移除、Agent 绑定、`skill_exec` 选择、列表复选和批量删除。
