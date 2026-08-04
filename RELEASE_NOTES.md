# 🚀 v0.2.0

📅 发布日期：2026-08-04

## ✨ Features

- 新增第一方 agent-hub-cli 管理 CLI 与 agent-hub-maintenance 维护 Skill，并随 Hub 镜像内置；Hub 启动后自动创建或更新该 Skill 与 CLI Package。
- Runtime 只为绑定了维护 Skill 的会话注入 Hub 内部地址与只读维护 API Key 文件，其余 skill_exec 会话保持隔离。
- 支持通过 AGENT_HUB_MAINTENANCE_AGENT_ID 自动将内置维护 Skill 绑定到指定维护智能体。

## 🐛 Bug Fixes

- 修复会话活动“处理 N 秒”计时受浏览器与服务器时钟偏差影响的问题，改为以服务器事件时间为锚点持续递增。

## 🔒 Security

- 维护 API Key 不写入 Skill Package、数据库或会话历史，仅以只读 secret 挂载，并受 Landlock 文件级读取限制。

## 🛠️ Maintenance

- 新增 compose.maintenance.yml 生产维护部署 override 与维护 CLI 使用文档。

## 👥 Contributors

- yiiilin
