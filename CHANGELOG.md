# Changelog

本文件记录 Agent Hub 各正式版本面向使用者的变化。

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
