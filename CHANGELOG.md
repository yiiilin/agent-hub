# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

本文件记录 Agent Hub 各正式版本面向使用者的变化。

## [0.3.11] - 2026-08-15

### Fixed

- 工具结果 `read` 接口 range 模式按 UTF-8 字节边界对齐：分页不再产生替换字符（U+FFFD）、偏移不再漂移；`limit` 小于多字节字符时返回至少一个完整字符，`next_offset` 始终前进（分页不死循环）。

## [0.3.10] - 2026-08-15

### Fixed

- 工具结果 range 读取 UTF-8 边界对齐（DB/S3 两路径统一，S3 前后各多取最多 3 字节），EOF 返回空页。

## [0.3.9] - 2026-08-15

### Fixed

- 大结果客户端工具提交 500 根因修复：截断后的结果不再破坏续接解析——`result_payload` 改为自包含包装层 `{tool_call_id, tool_name, result}`（与模型输入同构），截断时 `output` 直接保留前 32KB 文本、`truncated` 与 `artifact_ref` 标识截断与完整内容；多字节 UTF-8（中文等）结果在截断阈值处不再触发 `String::truncate` panic。
- 续接 run 模型输入的 plural 工具结果增加 64KB 总量预算：最新结果优先完整展开，超预算的旧结果以可读占位展示（保留工具身份与 `agent_hub_integration_tool_result_read` 读取指引）。
- 未归档工具结果 `read` 接口（size/range/full）改为直接从 DB 提供完整内容——亚阈值结果被预算占位后仍可取全文，不丢数据。

### Internal

- 迁移 0015：存量 client 工具结果 `result_payload` 包一层（以 `run.client_instance_id` 判定 client 路径，runtime 任意 JSON 结果不受影响）。
- 回归测试新增：截断 DTO 外壳端到端（DB 形状/事件协议/singular+plural 模型输入/S3 归档）、UTF-8 边界 panic、re-claim terminal、plural 预算裁剪、迁移 0015、预算占位可读。

## [0.3.8] - 2026-08-14

### Added

- 硬停止（force-stop）：运行中会话可强制停止（控制台与第三方 external 会话均支持）——命令经 WebSocket 投递并带 ACK/重试与 5 分钟超时兜底；停止时打包工作区快照上传，会话下次消息自动基于快照无损恢复。
- 升级无感化：Runtime 收到 SIGTERM 后先停引擎、再逐会话打包上传（管理端可见 bundle 同步进度），Compose 增加 `stop_grace_period`；配合会话恢复体系（reap 阈值放宽、bundle 同步状态、补丁消息端点），恢复中的会话可基于本地工作区或 bundle 恢复。
- 第三方应用会话预指令（prepend_instructions）：创建会话时一次性写入、不可变；同值重放忽略、异值返回 400。
- 管理端展示 bundle 打包进度（打包状态与剩余数量）。
- INFO 日志：Session Bundle checkpoint 提交成功（含生成号/大小/排队与归属释放）、client tool claim/result（tool_call_id/会话/状态/耗时）。
- OpenAPI 文档补齐三条缺失路由（会话消息上传、消息附件绑定、runtime 附件下载）。

### Fixed

- 停止链路四项硬伤修复与安全加固（排队 run 劫持、按钮依赖过期状态、stop 与 run 完成竞态、external 会话只读校验等）。
- 快照失败语义与 ACK 枚举加固：失败/丢失/未知状态按协议处理，不再确认 pending 操作。
- 恢复会话 turn_started 置 online 时未清 `recovery_source` 导致约束冲突。
- 恢复重建的 Pi 会话 jsonl 属主/权限错误导致 Pi 启动即退出。
- force release/reap 释放会话时清空 bundle 归档，消除 claim 永久阻塞。
- runtime drain 顺序修复：先收集会话再停引擎；drain 前先 fail 活动 run。
- reaper 释放 restoring 会话时清 `recovery_source`（防约束 23514）。
- `--ignored` 测试全量修复：fixture 适配恢复体系约束、生产 UPDATE 补齐约束列。

### Internal

- clippy `-D warnings` 清零（死代码删除与 lint 修复）；admin 模型选择测试与规则语义对齐。

## [0.3.7] - 2026-08-12

### Added

- 第三方工具大结果归档：工具结果超过 32KB 不再本地截断或拒绝——32KB 到 `max_tool_result_bytes`（系统参数，默认 4MB）之间的结果全文归档到对象存储，DB 与模型上下文只保留 32KB 截断版 + 摘要（含原始大小与读取指引）；超过硬上限的结果仅截断并明确告知"未归档"。归档上传带指数退避重试，失败降级不阻断工具调用。
- Pi 新增 `agent_hub_integration_tool_result_read` 工具：`size`（元数据）、`range`（有界文本切片 + 翻页）、`file`（全量写入工作区返回路径）三种模式读取归档结果。
- 会话详情中截断的集成工具结果显示"查看完整结果"链接（新标签页打开全量）。
- 会话删除时级联清理工具结果归档对象。
- 智能体端点暴露：每个智能体可声明允许被哪些端点使用（console / integration / automation），默认全部打开；控制台列表、Integration App 绑定、自动化创建分别按声明校验。
- Browser SDK 不再本地截断大工具结果（原样提交，由后端归档机制接管）；`/api/client/attachments` 上传与下载路由。

### Fixed

- run 正常 completed 时也发送终态 status 事件（此前只在非 completed 时发送），客户端可据此判定 run 真正结束。
- tool result broker 收到停止信号后不再触发 unreachable panic（此前会导致 runtime 崩溃重启与 run 状态错乱）。
- 会话活动合并时保留截断工具结果的"查看完整结果"链接（tool_request 先建活动、结果后合并的时序下不再丢失）。

## [0.3.6] - 2026-08-11

### Added

- 可嵌入的 `agent-hub-chat` Web Component：统一 iframe widget 的嵌入方式，普通页面可直接引入组件使用。
- 附件上传进度条：composer 选择附件与随消息发送时显示 multipart 上传进度。
- 附件大小限制改为系统参数：管理台"系统参数"可配置单文件上限与会话累计上限，上传校验实时读取配置。
- 附件选择预检：超过单文件上限的文件在选择时立即提示，不会进入待发送列表。

### Fixed

- 运行中会话的"停止"按钮失效：停止目标会被排队中的新 run 劫持、按钮依赖过期会话状态不显示、run 恰好在 stop 请求前完成时返回 409 且 UI 卡住。现在停止与事件流统一以活动 Turn 的 run 为目标；stop 对已终态 run 幂等返回，前端收到终态立即收敛按钮与思考状态。
- 会话运行中发送消息无法引导对话：steer 消息的确认与投递已修复，消息进入当前 run 的上下文。
- 执行命令活动显示 `{}`：completed 阶段事件的 command 字段为空对象序列化，现已过滤并正确显示真实命令。
- 长输出折叠方向反了：折叠区此前显示最旧内容（CSS 截断），现改为底部视窗展示最新内容，展开/收起行为一致。
- 附件发送失败后进度卡在 92%：失败时回退为可重试的待发送状态。
- 带附件消息的请求体上限过小导致上传失败；匿名 widget 的同源请求修复。

### Internal

- main.rs 拆分为领域模块（56k 行 → 12k 行），测试同步迁移到各领域模块；行为不变。

## [0.3.5] - 2026-08-10

### Added

- 技能共享：技能支持与智能体一致的可见性（private / public_to / public），所有用户可创建公开技能；遵循"可见即可读可挂载，可写才能改删"——共享技能可挂载到其他用户的智能体，编辑与删除仅限 owner 与 admin。
- 智能体、技能、模型连接列表显示创建人（owner email）。
- 自动化可绑定当前用户可调用的公共智能体（public / public_to），member 也能为公共智能体创建、编辑并触发自动化。
- Widget 会话支持通过 Client API 删除（`DELETE /api/client/sessions/{session_id}`）。
- Client 会话事件历史接口支持 `limit` 参数；断线重连与会话恢复后自动恢复 pending 的 Client 工具调用。
- 登录页邮箱与密码占位符可在认证配置中自定义（管理台 → 认证配置，留空回退内置文案）。

### Fixed

- 思考内容折叠严格按 15 个视觉行截断（自动换行、空行均计入），折叠计数与实际显示一致。
- 事件历史 limit 查询重复 ORDER BY 导致 500 的问题。
- 智能体详情页左侧概览不再显示指令内容。
- 登录页不再预填默认账号密码。

## [0.3.4] - 2026-08-08

### Fixed

- 修复 Client Tool 续跑 run（`source = integration:tool_result`）中工具请求无法被浏览器认领、事件流被拒绝授权、运行无法停止的问题：此前链式工具调用会返回 404 并让会话卡在 `waiting_tool`，现与 widget run 一样支持认领、流授权与停止，Executor 的 `client_instance_id` 校验保持不变。

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

[Unreleased]: https://github.com/yiiilin/agent-hub/compare/v0.3.8...HEAD
[0.3.8]: https://github.com/yiiilin/agent-hub/compare/v0.3.7...v0.3.8
[0.3.1]: https://github.com/yiiilin/agent-hub/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/yiiilin/agent-hub/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/yiiilin/agent-hub/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yiiilin/agent-hub/releases/tag/v0.1.0
