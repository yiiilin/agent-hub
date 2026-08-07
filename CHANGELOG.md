# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

本文件记录 Agent Hub 各正式版本面向使用者的变化。

## [0.3.3] - 2026-08-07

### Added

- 管理员（admin）可查看、修改并删除 super_admin 创建的智能体，并查看其 Run 历史；普通 member 仍不能访问私有超管智能体。
- admin 编辑他人智能体时只能保留原有 Personal 模型选择或改用 Global 连接，不能查看或改绑 super_admin 的 Personal 连接。
- 工具输出事件上限 32KB：超出后事件只保留尾部并带 `[output truncated: N bytes]` 标记；增量输出超过累计上限后停止流式。
- `skill_exec` 大输出落盘：完整 stdout/stderr 写入会话隔离临时日志（单流 256MB、会话共享 512MB 硬上限），工具结果返回 `stdout_full_path` / `stderr_full_path`。

### Fixed

- 修复工具输出含二进制 NUL 字节导致事件写入 500、Run 失败、会话现场丢失的问题；Runtime 与 Hub 全部 run_events / JSONB 写入路径统一清洗 NUL。
- 修复新 Run `initial_message`、integration/client tool 结果与 tool arguments 等旁路写入未清洗 NUL 的隐患。
- Runtime 事件接口新增大小护栏，超限事件返回 400 而非写入数据库。

### Security

- 关闭 admin 通过直接构造 PATCH 把 super_admin 智能体改绑到任意 Personal 模型连接的口子。

## [0.3.2] - 2026-08-07

### Added

- 聊天附件：文件/图片上传、图片内联预览与点击放大、文件下载；控制台与 Widget（含匿名）均支持，私有会话按会话身份鉴权。
- 草稿一条消息即创建会话：消息文本与附件文件通过单个 multipart 请求一次性提交（新建会话与已有会话均支持）。
- 输入框支持 Ctrl+V / 右键粘贴图片与文件，待发送列表显示图片缩略图。
- 内置 `vision_analyze` 工具：读取会话工作区图片原图，经现有模型代理发送视觉请求；模型连接新增可选 `vision_model_id`，Hub 自动切换视觉模型，未配置时使用智能体当前模型。
- 新增 README 与 MIT 许可证。

### Fixed

- 修复 super_admin 创建的公共智能体对普通用户不可见（列表与详情均修复），私有超管智能体仍保持隐藏。
- 修复 vision_analyze 无法解析 `message` 类型输出的问题，mimo-v2.5 等视觉模型可正常返回分析结果。
- 修复既有集成测试套件中因智能体未绑定模型、心跳/reaper 语义变更等原因导致的失败。

## [0.3.1] - 2026-08-06

### Added

- 会话自动标题：用户发送第一条消息后，Hub 并行调用智能体模型识别意图并生成约 15 字标题；发送消息后控制台自动刷新列表标题。
- 会话管理：会话列表右键可重命名、删除会话，并可复制会话链接直接打开指定会话。
- 崩溃现场抢救：Runtime 崩溃后本地工作区仍在时，重启后自动重新打包上传 Session Bundle，会话完整恢复；本地现场已丢失时提示“智能体环境数据丢失，但对话历史还在”，会话仍可继续对话。
- Runtime 上送重试与幂等确认：事件、Run 完成、会话释放等上送操作按 0/1/3/7/10 秒重试，Hub 按事件 ID 幂等确认，心跳对账作为兜底。
- 恢复机制增强：心跳不再被陈旧会话阻塞；saving/运行中会话在 Runtime 崩溃后自动回收；可恢复会话回到运行队列继续执行。

### Changed

- 会话标题提示词优化为“意图/任务主题”概括并附带示例，避免生成“我能做什么”式回应标题。
- 环境丢失提示改在会话底部展示，用户发送新消息后自动消失。
- 会话恢复失败不再设为只读，可从 Hub 历史重建工作区继续对话。
- 活动预览默认只显示最新 15 行，超长单行按自动换行计数并折叠，不再撑爆宽度。

### Fixed

- 修复心跳 409 死锁导致会话一直停留在“等待运行节点分配”。
- 修复 saving 状态会话在 Runtime 崩溃回收时违反检查约束导致整个回收失败。
- 修复抢救 Bundle 重放上传被错误拒绝（义务已清除后重放 409）。
- 修复会话底部跟随失效与历史加载时滚动位置错位。
- 修复技能 Markdown 编辑器中代码块解析报错（如 `text` 语言代码块）。
- 修复 CLI 更新智能体时清空秘钥声明的问题；存量 delta 事件已合并迁移。

### Security

- 环境丢失提示与恢复流程不暴露内部路径或凭据。

## [0.3.0] - 2026-08-05

### Added

- 新增用户级秘钥变量：用户可创建 value/file 型秘钥，智能体声明所需秘钥并在首次使用时申请授权；已授权秘钥以 `AGENT_SECRET_*` 环境变量和 `/agent-state/secrets` 文件形式注入，Widget 与第三方接入同样支持。
- 每个会话独立沙箱：工作区映射为 `/workspace`、独立临时目录映射为 `/tmp`、engine-state 映射为只读 `/agent-state`；Pi 以非特权 UID 10001 运行，受 Landlock 与私有 mount namespace 约束。
- Skill 包与文档合并为单目录 `.pi/agent/skills/<slug>/`，包内除 `SKILL.md` 外任意文件均可作为 `skill_exec` 程序执行。
- 会话历史分页：控制台与 Widget 首屏只加载最新 10 条消息，向上滚动时再加载更早历史。
- 会话活动步骤实时展示（思考、工具调用、命令输出），可见回复后自动折叠为“处理 N 秒”。
- Runtime 镜像内置 ssh、python、git。

### Changed

- Model Gateway 默认上游首字超时调整为 60 秒。

### Fixed

- 修复推理片段 item id 重复、授权秘钥变化后 Pi 未刷新、秘钥指导文本在刷新时残留等问题。
- 修复会话 Bundle 恢复后文件属主错误，恢复的工作区可继续写入。

### Security

- 会话秘钥文件以 `root:agenthub 0440` 只读发布，防止通过 truncate/chmod 绕过只读边界。
- 控制创建的 Pi 配置、扩展与技能源统一 `root:agenthub 0440/0550`，沙箱用户无法改写或替换；消除目录/文件属主竞态与符号链接绕过窗口。
- `skill-exec` catalog 与执行源只读，会话 Bundle 恢复路径在离线状态下修复属主。

## [0.2.0] - 2026-08-04

### Added

- 新增第一方 agent-hub-cli 管理 CLI 与 agent-hub-maintenance 维护 Skill，源码位于仓库内，CLI 二进制随 Hub 镜像内置。
- Hub 启动后自动创建或更新内置维护 Skill 并上传 CLI Package；配置 AGENT_HUB_MAINTENANCE_AGENT_ID 后可自动绑定到指定维护智能体。
- Runtime 只为绑定了维护 Skill 的会话注入 AGENT_HUB_HUB_URL 和 AGENT_HUB_API_KEY_FILE，其余 skill_exec 会话保持隔离。
- 新增 compose.maintenance.yml 生产维护部署 override 与维护 CLI 使用文档。

### Changed

- 会话活动计时改为以服务器事件时间为锚点递增，避免浏览器与服务器时钟偏差导致“处理 N 秒”跳变或回落。

### Security

- 维护 API Key 不写入 Skill Package、数据库或会话历史，仅以只读 secret 挂载，并受 Landlock 文件级读取限制。
- 维护 Skill 默认不自动绑定任何智能体；建议只绑定私有维护助手，不要绑定公开智能体。

## [0.1.0] - 2026-07-31

### Added

- 提供由 Hub、独立 Runtime 和 Model Gateway 组成的完整部署，Hub 同时托管管理台静态资源。
- 使用 Pi standalone 驱动隔离会话，支持工作区恢复、Skill Package、受限工具执行、Runtime 排空与 Session Bundle。
- 支持全局和个人模型连接、Responses/Chat Completions/Anthropic Messages 协议转换、用量与错误历史统计。
- 提供账号密码和单目录 LDAP 登录、管理员权限、用户身份管理及可配置登录策略。
- 提供 Integration App、认证或匿名 Widget、Browser SDK、历史会话和 at-most-once Client Tool 调用。
- 提供 Agent、Skill、MCP、Automation、API Key、Runtime、模型与会话管理界面，以及 Markdown 编辑和流式对话展示。
- 提供第三方平台接入指南、OpenAPI 页面和无人值守 API/浏览器 QA 场景。

### Changed

- 生产 `compose.yml` 默认拉取同一 `0.1.0` 版本的 GHCR Hub、Runtime 和 Model Gateway 镜像。
- Release workflow 在登录 GHCR 前扫描完整 Git 历史、候选镜像配置、每个最终 image layer 及二进制可打印字符串，不上传扫描报告、Buildx cache 或 build record。

### Security

- 模型密钥、OAuth 凭证、Runtime 凭证和 Bundle 存储凭证均由部署环境注入，不写入源码或镜像。
- Release 工作流在 GHCR 鉴权和发布前完成全历史与镜像凭证扫描，且不上传原始扫描报告。
- LDAP QA 私钥改为测试环境启动时临时生成，不再保存在 Git 中。

[Unreleased]: https://github.com/yiiilin/agent-hub/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/yiiilin/agent-hub/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/yiiilin/agent-hub/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/yiiilin/agent-hub/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yiiilin/agent-hub/releases/tag/v0.1.0
