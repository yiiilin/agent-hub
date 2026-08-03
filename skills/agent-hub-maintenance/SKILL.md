---
name: agent-hub-maintenance
description: Diagnose and maintain an Agent Hub deployment through its management API.
---

# Agent Hub 维护

你被允许通过受控的 skill_exec 客户端管理 Agent Hub。只使用本 Skill 提供的 bin/agent-hub
程序，不把 skill_exec 当作通用 shell，也不执行 bin/ 之外的任何程序。

## 环境

Runtime 只对绑定了本 Skill 的会话注入：

- AGENT_HUB_HUB_URL：Hub 内部地址。
- AGENT_HUB_API_KEY_FILE：只读 API Key 文件路径；由部署方挂载，不写入 Skill 包或会话。

客户端从环境变量自动读取配置。若命令报缺少 Hub URL 或 API Key，说明当前会话未获得维护凭据，
停止并报告，不要尝试猜测或伪造凭据。

## 常规流程

1. 先执行 health 确认身份和连通性。
2. 修改前先 list/get 查看现状。
3. 修改类操作（例如 agents update）执行前，先向用户说明将变更什么并等待确认；
   只修改用户明确要求的内容。
4. 变更后重新 get 或 list 验证结果。

## 命令

所有命令通过 skill_exec 调用，skill 固定为 agent-hub-maintenance，program 固定为
bin/agent-hub，其余参数放在 args 中。常用命令：

- health：验证登录并显示当前用户。
- agents list / agents get <id>：查看智能体。
- agents update <id> --name ... --visibility ... --instructions-file ... --add-skill <id> --remove-skill <id>：修改智能体。
- sessions list：查看会话。
- runtimes list：查看运行节点。
- models list：查看模型连接。
- skills list / skills create / skills package upload / skills delete：管理技能。

输出为 JSON。不要输出或记录 API Key；agents get 返回的 has_management_api_key 等布尔字段
不是秘密，但任何 token/api_key/secret 字段都不得外泄。

## 边界

- 这是第一方维护通道，不是通用终端；禁止用其执行任意命令。
- 默认只读；写操作必须得到用户确认，且只做用户要求的变更。
- 不要修改与本任务无关的智能体、模型、用户或部署配置。
- 若 API 返回 401/403，停止并说明权限不足，不要重试猜测凭据。

