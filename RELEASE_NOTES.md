# 🚀 Agent Hub v0.4.4

📅 发布日期：2026-08-18

## 🐛 Bug Fixes

- 对话 live 活动步标签（如中文"执行命令"）在窄容器下不再被长命令摘要挤成逐字竖排，保持单行。

## ✨ Changes

- Run 以 failed 收尾时，后端按 run_id 从 `model_call_errors` 取已脱敏错误详情，在 `status` 事件前注入 `error` 事件透传前端；运行时失败终态以受控文案“模型执行失败”上送 `error` 事件。
- `model_call_errors` 增加 `run_id` 关联与索引（迁移 0016）。

## 👥 Contributors

- yiiilin
