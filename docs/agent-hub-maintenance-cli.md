# Agent Hub 维护 CLI 与 Skill

agent-hub-cli 是 Agent Hub 的第一方管理 CLI，agent-hub-maintenance 是配套 Skill
Package。两者都放在本仓库：CLI 源码在 crates/agent-hub-cli/，Skill 内容在
skills/agent-hub-maintenance/，bin/agent-hub 由构建脚本生成，不提交二进制。

## 凭据模型

- CLI 通过 Hub 管理 API 认证，使用现有 API Key。
- API Key 不写入 Skill Package、数据库或会话历史。
- 部署方把 API Key 放入只读文件，例如 secrets/agent-hub-maintenance-token，
  并用 compose.maintenance.yml 挂载到 Runtime。
- Runtime 只在会话绑定了 agent-hub-maintenance Skill 时，向该会话的
  skill_exec 子进程注入 AGENT_HUB_HUB_URL 与 AGENT_HUB_API_KEY_FILE；
  其他会话的 Skill 子进程既看不到环境变量，也没有读取该文件的 Landlock 权限。

## 构建与安装

执行：

    ./scripts/install-agent-hub-maintenance.sh

需要环境变量：

- AGENT_HUB_HUB_URL：Hub 地址。
- AGENT_HUB_API_KEY 或 AGENT_HUB_API_KEY_FILE：安装用 API Key。
- AGENT_ID：私有维护智能体 ID。

脚本会构建 CLI、创建/更新 Skill、上传 Package，并把 Skill 绑定到指定智能体。

## 生产部署

1. 创建 API Key 文件：

    mkdir -p /root/compose/agent-hub/secrets
    umask 077
    printf '%s' 'ahk_...' > /root/compose/agent-hub/secrets/agent-hub-maintenance-token

2. 使用维护 override 启动：

    docker compose -f compose.yml -f compose.maintenance.yml up -d

3. 只把 agent-hub-maintenance Skill 绑定到私有“Agent Hub 维护助手”，
   不要绑定公开智能体。

## 安全说明

- 当前 API Key 继承所属用户权限；使用前确认该 Key 属于 super_admin 且只用于维护。
- 未来建议为管理 Key 增加细粒度 scope，进一步缩小 CLI 的写权限。
- 宿主机 Docker/Compose 操作不在本 Skill 范围内；需要时另做窄权限 Host Operator。

