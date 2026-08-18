# 🚀 Agent Hub v0.4.3

📅 发布日期：2026-08-18

## 🐛 Bug Fixes

- 修复 `tool_result_read` 工具失效：Pi 扩展的 `execute` 签名与 Pi API（`execute(toolCallId, params, signal)`）不匹配，broker 请求缺少 `tool_call_id` 字段，被截断的工具结果永远无法读取全文（部署日志"总是被截断"的根因）；新增源码级回归测试。
- 模型代理请求体上限提升至 64MB，并记录请求体大小。

## ✨ Changes

- 移除内部 Model Gateway（Go/Bifrost 协议转换层），Hub 直连上游模型端点，只做认证改写与安全头过滤，请求体原样透传、不再做任何协议转换。
- Pi 按 Run Binding 的 API 类型原生直连：`openai-responses` / `openai-completions` / `anthropic-messages`（含 Anthropic 根 URL 修正，避免 `/v1/v1/messages`）。
- 用量与错误记账扩展至三协议：Chat Completions（`finish_reason` + `prompt_tokens`，流式在 `[DONE]` 后落账）与 Anthropic Messages（`message_start`/`message_delta`/`message_stop`，cache 口径计入 input）；终态映射与 Pi 一致（`content_filter`/`network_error`/`refusal`/`sensitive` 等记为失败）。
- 会话标题生成与模型连接测试按协议构造请求并解析响应，不再仅支持 Responses。
- 模型代理路径与 Run Binding 协议强制一致（不匹配 fail closed）；无法表示为 HTTP 头的凭据直接拒绝发送（fail closed），认证头标记敏感防日志泄露。
- 采样覆盖（`temperature`/`top_p`）随模型网关移除而停用（前端输入框禁用并提示）；`max_completion_tokens`/`max_tokens` 仍作为 Pi 的 maxTokens 上限生效。
- 浏览器 SDK（`@agent-hub/client`）随 GitHub Release 发布 tarball 资产（含 SHA256SUMS），第三方应用可按固定版本直接安装。
- SDK 许可证由 `UNLICENSED` 调整为与仓库一致的 `MIT`，包内附带 `LICENSE` 文件。

## 👥 Contributors

- yiiilin
