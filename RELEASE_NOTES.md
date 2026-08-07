# 🚀 Agent Hub v0.3.3

📅 发布日期：2026-08-07

## ✨ Features

- 管理员可管理 super_admin 创建的智能体：列表、编辑、删除与 Run 历史；member 仍无法访问私有超管智能体。
- 工具输出事件统一截断到 32KB（保留尾部并标记），数据库与页面不再被超长输出拖垮。
- `skill_exec` 超 1MB 输出自动落盘到会话隔离日志（单流 256MB、会话 512MB 上限），并把完整日志路径返回给智能体。

## 🐛 Bug Fixes

- 修复工具读取二进制文件（如 ELF）时 NUL 字节导致事件 500、Run 失败、会话现场丢失的问题。
- 修复 integration/client tool 结果与 initial_message 等写入路径未清洗 NUL 的隐患。
- Runtime 事件接口增加大小护栏。

## 🔒 Security

- 管理员无法再把他人智能体改绑到任意 Personal 模型连接，只能保留原选择或使用 Global 连接。

## 👥 Contributors

- yiiilin
