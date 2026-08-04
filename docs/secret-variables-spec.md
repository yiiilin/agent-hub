# 个人密钥变量 Spec

## 范围

个人密钥变量（Personal Secret Variables）是用户拥有的凭据，分为 value 与 file 两种类型，以密文保存在 Hub 数据库中。Agent 只声明它需要的变量名、类型和用途说明，不绑定任何具体用户的密钥；Run 开始时，Hub 把 Agent 声明与发起用户的已授权密钥取交集后注入 Runtime。

## 个人密钥

- 每个密钥由 `owner_id`（Hub 用户）与 `name` 唯一确定；名称满足 `^[A-Z_][A-Z0-9_]*$`，最长 128 字符。
- `kind` 为 `value` 或 `file`：
  - `value`：1 到 8192 字节的字符串。
  - `file`：1 字节到 1 MiB 的二进制文件，附带展示用 `file_name`（最长 255 字符且不含 `/`）、`file_size_bytes` 与 `file_sha256`。
- 明文只存在于请求、加密瞬态以及 Run claim/下载链路中；`GET /api/secrets` 和创建/更新响应只返回元数据，不返回值或文件内容。
- value 密文保存在 `value_ciphertext`/`value_nonce`，文件以 base64 明文加密保存在 `file_ciphertext`/`file_nonce`，均使用部署级对称主密钥与独立 nonce；数据库约束保证两种 kind 的存储形状互斥。

## 智能体声明

- `POST /api/agents` 与 `PUT /api/agents/{id}` 接受 `secret_declarations: [{ name, kind, description }]`。
- 校验规则：名称格式同上且在同一 Agent 内唯一；`kind` 只能是 `value` 或 `file`；`description` 最长 512 字符。
- 声明持久化在 `agent_secret_declarations`，随 Agent DTO 返回，并作为执行配置的一部分参与指纹与 revision 计算；修改声明会递增配置 revision。
- 声明不包含任何用户密钥值，也不指向具体用户。

## 授权

- 授权记录为 `secret_grants(user_id, agent_id, secret_name)`，默认持久化记住，不是一次性批准。
- 用户只能授权自己拥有的密钥，且密钥名称必须是该 Agent 已声明的名称。
- Console 在提交 Hub-native Session 消息前检查：Agent 声明中用户已拥有但尚未授权的条目会以 `428 Precondition Required` 返回，响应体为 `details.secret_grants_required`；管理台提示“允许并继续”，先创建 grant 再重发消息。
- 删除个人密钥会同时删除该用户对该名称的全部 grant。

## 注入

- Runtime claim 时，Hub 以 Session owner 为 subject 逐个检查 Agent 声明：
  - 仅当用户拥有同名密钥且存在 grant 时才解密并注入；未授权或未拥有的一律不注入。
  - `value` 加入 `ClaimRunResponse.secret_values`（`name` + 解密后的值）。
  - `file` 加入 `ClaimRunResponse.secret_files`（`name`、`size_bytes`、`sha256`），文件内容不在 claim 中传输。
- Runtime 准备阶段在 `engine-state/secrets/` 落盘文件密钥：目录 `0700`、文件 `0600`、`create_new` 防覆盖、写后 `sync_all`；下载端点要求 Runtime Bearer 与 `x-agent-hub-ownership-generation`，并校验文件属于该 Run、Agent 声明为 `file` 且 grant 仍然存在。
- Pi 进程启动时注入：
  - `secret_values` → `AGENT_SECRET_<NAME>` 环境变量。
  - `secret_files` → `AGENT_SECRET_FILE_<NAME>` 环境变量，值为 Session 私有 `engine-state/secrets/<NAME>` 路径。
- 下载的文件在 Runtime 侧校验大小与 SHA-256 必须与 manifest 一致（单文件上限 1 MiB），不一致则拒绝启动 Run。

## 撤销

- 撤销 grant（`DELETE /api/secret-grants/{agent_id}/{secret_name}`）或删除密钥后，后续 Run claim 不再注入该密钥。
- 已注入的当前 Pi 进程在 Session 结束或被重启前仍保留环境变量与已落盘文件；Bundle 恢复不会恢复任何密钥。

## 沙箱

- `engine-state/secrets` 的 Landlock 只读规则仅在工具集包含 `read`/`grep`/`find`/`ls` 之一且不包含 `bash`/`edit`/`write` 时授予（list + read file），不授予写、改名或执行权限。
- 包含 `bash`、`edit` 或 `write` 时，Pi 的 Landlock 规则不包含 secrets 目录，任何路径读取都会被拒绝。
- Skill exec 进程使用独立的 Landlock 规则，不授予 secrets 目录，技能代码不能读取密钥。
- secrets 目录位于 `engine-state/`，不属于 workspace；普通 workspace 工具不能越界读取其他 Session 或 engine-state 文件。

## Bundle 排除规则

- Session Bundle 只包含 workspace 与匹配的 Pi Native Session JSONL；`engine-state/secrets/` 不进入 Bundle。
- 环境变量注入值不写入任何 Session 文件。
- 用户主动复制到 workspace 的密钥内容属于用户数据，会随 workspace 进入 Bundle，不在 Runtime 排除范围内。

## API 端点摘要

| 方法与路径 | 说明 |
| --- | --- |
| `GET /api/secrets` | 列出当前用户的密钥元数据 |
| `POST /api/secrets` | 创建 value/file 密钥 |
| `PUT /api/secrets/{secret_id}` | 更新密钥内容（kind 保持不变） |
| `DELETE /api/secrets/{secret_id}` | 删除密钥及其全部 grant |
| `GET /api/secret-grants?agent_id=` | 列出当前用户的 grant（可按 Agent 过滤） |
| `POST /api/secret-grants` | 批量创建 grant（名称必须已声明且已拥有） |
| `DELETE /api/secret-grants/{agent_id}/{secret_name}` | 撤销单个 grant |
| `GET /api/runtime/runs/{run_id}/secrets/{name}` | Runtime 下载文件密钥（Bearer + ownership generation） |
| `POST /api/sessions/{session_id}/messages` | Console 发消息；缺授权时返回 428 + `details.secret_grants_required` |

## Widget 与第三方一视同仁

- Console、认证 Widget/第三方 Integration 与 Automation 发起的 Run 都使用同一套用户级 grant：claim 以 Session owner 为 subject，与消息来源无关。
- 未授权密钥在任何通道都不会注入；匿名公开 Widget 的 Session owner 是 Agent 归属用户，因此只可能注入该用户已授予该 Agent 的密钥，访客没有自己的个人密钥。
- Console 特有的 428 交互是管理台可用性增强，不是授权边界差异。
