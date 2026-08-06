# 🚀 v0.3.1

📅 发布日期：2026-08-06

## ✨ Features

- 会话级隔离沙箱：每个会话独立工作区（`/workspace`）、独立临时目录（`/tmp`）与只读引擎状态（`/agent-state`），Pi 以非特权用户运行。
- 用户秘钥变量：创建 value/file 型秘钥，智能体声明并按需授权，运行时注入环境变量与 `/agent-state/secrets` 文件。
- Skill 包与文档合并为单目录，包内任意文件（除 SKILL.md）都可作为 `skill_exec` 程序执行，不再强制 `bin/` 前缀。
- 会话历史分页：首屏仅加载最新 10 条消息，向上滚动加载更早历史（控制台与 Widget）。
- 会话活动步骤实时展示（思考、工具调用、命令），回复后自动折叠。
- Runtime 镜像内置 ssh、python、git。

## 🐛 Bug Fixes

- 修复授权秘钥变化后 Pi 未刷新、秘钥指导残留、推理片段 ID 重复等问题。
- 修复 Bundle 恢复后文件属主错误，恢复会话可继续写入。

## 🔒 Security

- 秘钥文件 `root:agenthub 0440` 只读发布，防止 truncate/chmod 绕过。
- Pi 配置、扩展与技能源统一 `root:agenthub 0440/0550`，消除属主竞态与符号链接绕过。
- `skill-exec` 执行源与 catalog 只读，沙箱用户无法篡改。

## 👥 Contributors

- yiiilin
