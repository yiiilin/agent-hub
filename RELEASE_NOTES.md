# 🚀 Agent Hub v0.3.13

📅 发布日期：2026-08-16

## 🐛 Bug Fixes

- 大结果读取链路完整修复：Pi 工具白名单暴露 `tool_result_read`，模型可读取截断结果的全文归档。
- 工具结果读取 broker 生命周期改为由会话进程持有；凭证共享轮换自动生效，`file` 模式写入当前会话工作区。
- 读取接口联表校验 `hub_session_id`、Runtime 归属与 ownership generation，拒绝同 Runtime 跨会话读取。
- `tool_call_id` 改为严格 UUID，URL 查询参数结构化构造，避免 `?#` 注入篡改会话参数。
- Client 受管工具内部名统一为 `agent_hub_client_tool_{实际名}`，与 `tool_result_read` 使用一致命名。

## ✨ Changes

- Browser SDK 统一错误提交协议：工具超时或中断生成错误结果，提交失败缓存结果可安全重放。
- 绝对执行期限预留提交余量；`executing/unknown` 无结果时以 `tool_result_unknown` 收尾。

## 👥 Contributors

- yiiilin
