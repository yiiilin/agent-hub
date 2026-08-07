# 🚀 Agent Hub v0.3.2

📅 发布日期：2026-08-07

## ✨ Features

- 聊天附件：文件/图片上传、图片预览与放大、文件下载；控制台与 Widget（含匿名）支持。
- 草稿首条消息即创建会话，消息与附件单请求提交；支持 Ctrl+V/右键粘贴图片与文件并显示缩略图。
- 内置 `vision_analyze` 工具，模型连接可选视觉模型（`vision_model_id`）。
- 新增 README 与 MIT 许可证。

## 🐛 Bug Fixes

- super_admin 的公共智能体对普通用户可见（列表与详情）。
- vision_analyze 支持 `message` 类型响应，mimo-v2.5 等视觉模型正常返回结果。
- 修复既有集成测试失败。

## 👥 Contributors

- yiiilin
