# 🚀 Agent Hub v0.3.7

📅 发布日期：2026-08-12

## ✨ Features

- **第三方工具大结果归档**：结果超过 32KB 不再本地截断或整体拒绝——32KB ~ `max_tool_result_bytes`（系统参数，默认 4MB）之间全文归档到对象存储，模型上下文只保留 32KB 截断版 + 摘要（原始大小 + 读取指引）；超过硬上限仅截断并明确告知"未归档"。上传带指数退避重试，失败降级不阻断工具调用。
- **Pi 读取工具 `agent_hub_integration_tool_result_read`**：`size`（元数据）/ `range`（有界切片 + 翻页）/ `file`（全量写入工作区）三种模式读取归档结果。
- **智能体端点暴露**：每个智能体可声明允许被哪些端点使用（Console / Integration / Automation），默认全部打开——控制台列表、Integration App 绑定、自动化创建分别校验。
- 会话详情截断结果提供"查看完整结果"链接；会话删除级联清理归档。
- Browser SDK 大工具结果原样提交（后端归档接管）；`/api/client/attachments` 上传下载。

## 🐛 Bug Fixes

- run 正常 completed 也发送终态 status 事件，客户端可判定 run 真正结束。
- tool result broker 停止后不再 panic（此前导致 runtime 崩溃重启）。
- 会话活动合并时序下不再丢失"查看完整结果"链接。

## 👥 Contributors

- yiiilin
