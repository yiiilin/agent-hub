# 第三方平台接入指南

本文面向需要把 Agent Hub 对话能力接入现有网站、业务系统或后端服务的开发者。字段级接口定义以控制台的“API 文档”和 `/openapi.json` 为准；Browser SDK 的完整生命周期与错误处理见 [Browser Client SDK 接入指南](./client-sdk-guide.md)，协议边界见 [Integration App、Browser SDK 与 Client Tool Spec](./integration-spec.md)。

## 选择接入方式

| 方式 | 适用场景 | 身份与会话 | 推荐程度 |
| --- | --- | --- | --- |
| 认证浏览器接入 | 第三方平台有自己的登录和可信后端 | 后端把已认证用户映射为 Hub User；支持历史会话、续聊和动态 Client Tools | 推荐 |
| 匿名浏览器或 Widget | 知识库、公开问答等无需登录的页面 | 不创建 Hub User；只能恢复当前匿名会话，不能发现历史会话 | 仅限无需身份的场景 |
| 服务端 API | 后端任务、机器人或平台自行实现完整聊天 UI | 使用 OAuth Application Token 和 Integration Session API | 后端工作流 |

不要把 `client_secret`、Application Token 或 Client Access Credential 放入 URL。第三方平台有后端时，应优先使用认证浏览器接入：长期 secret 只留在可信后端，浏览器只获得 15 分钟的短期 Client Access Credential。

## 接入前准备

1. 管理员在“管理 -> 外部平台”中创建并启用外部平台及认证渠道。
2. 准备一个可运行的 Agent，确认它已配置模型、可用 Runtime 和所需工具。
3. 在“接入应用”中创建 Integration App，选择外部平台、认证渠道和允许访问的 Agent。
4. 根据场景设置历史会话、精确 Origin、应用工具限制和 Client Tool 定义。
5. 创建后立即保存一次性展示的 `client_id` 与 `client_secret`。`client_secret` 只能进入服务端 secret manager；丢失后应轮换。轮换会阻止旧 secret 再签发新 Token，但已经签发的 Token 仍按自身到期时间和实时权限检查失效。

若 Client Tool 需要被模型调用，Agent 和 Integration App 都必须允许 `integration` 工具。最终可用工具是 Agent、应用和当前授权工具集合的交集。

## 认证浏览器接入

### 1. 可信后端签发浏览器 Credential

浏览器先生成标签页级 `clientInstanceId`，再请求第三方平台自己的后端。该后端从当前登录 Session 读取可信用户资料，并使用 Integration App 的 Basic Auth 调用 Hub：

```http
POST /api/client/access
Authorization: Basic base64(client_id:client_secret)
Content-Type: application/json
```

服务端 TypeScript 示例：

```ts
type SignedInUser = {
  id: string;
  tenantId: string;
  email: string;
  displayName: string;
};

export async function issueAgentHubAccess(
  user: SignedInUser,
  clientInstanceId: string,
) {
  const basic = Buffer.from(
    `${process.env.AGENT_HUB_CLIENT_ID}:${process.env.AGENT_HUB_CLIENT_SECRET}`,
  ).toString("base64");

  const response = await fetch(`${process.env.AGENT_HUB_URL}/api/client/access`, {
    method: "POST",
    headers: {
      Authorization: `Basic ${basic}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      agent_id: process.env.AGENT_HUB_AGENT_ID,
      client_instance_id: clientInstanceId,
      external_user_id: user.id,
      tenant_id: user.tenantId,
      display_name: user.displayName,
      email: user.email,
      attributes: {},
      client_tools: [],
    }),
  });

  if (!response.ok) throw new Error(`Agent Hub access failed: ${response.status}`);
  return response.json();
}
```

`email` 必填，且必须来自第三方平台已经认证的用户资料。后端还应自行决定 `agent_id`、`tenant_id`、用户属性和 Client Tool 集合，不得让浏览器任意扩权。

### 2. 浏览器使用 SDK

`@agent-hub/client` 当前不在公共 npm registry。可以从本仓库的 `sdk/typescript` 使用 workspace 依赖，或先执行 `npm pack`，再安装固定版本 tarball。

```bash
npm install /path/to/agent-hub-client-0.1.0.tgz
```

```ts
import { connect, type SessionEvent } from "@agent-hub/client";

const client = await connect({
  baseUrl: "https://hub.example.com",
  authorize: async ({ clientInstanceId, signal }) => {
    const response = await fetch("/api/agent-hub/access", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ clientInstanceId }),
      signal,
    });
    if (!response.ok) throw new Error(`authorize failed: ${response.status}`);
    return response.json();
  },
});

const session = client.sessions.draft();
await session.send("你好", {
  clientMessageKey: `message:${crypto.randomUUID()}`,
});

const subscription = session.subscribe((event: SessionEvent) => {
  if (event.type === "message" || event.type === "assistant") {
    console.log(event.content);
  } else if (event.type === "error") {
    console.error(event.code, event.message);
  }
});

// Later, during view teardown or logout:
subscription.dispose();
session.dispose();
client.dispose();
```

`draft()` 不会立即在 Hub 创建 Session。首条消息被接受后，Hub 才在同一事务中创建 Session、消息和 Run。

## 历史会话

认证应用开启“Widget 历史会话”后，SDK 可列出当前 App、Agent、Platform、Tenant 与 External Identity 范围内的会话：

```ts
if (client.historyEnabled) {
  const sessions = await client.sessions.list();
  const existing = sessions[0]
    ? client.sessions.existing(sessions[0].id)
    : client.sessions.draft();
  const messages = existing.isDraft ? [] : await existing.messages({ limit: 50 });
  console.log(messages);
}
```

关闭历史后，客户端不能列出会话；持有精确 Session ID 的当前页面仍可继续该会话。Credential 不绑定单个 Session，因此同一标签页可操作多个符合完整 scope 的 Session。不同 App、Agent、Platform、Tenant 或 External Identity 之间不能互相读取。

## Client Tool

有后端的应用在每次 `/api/client/access` 授权时提交完整的 `client_tools` 定义，浏览器只为已授权名称注册 handler。模型工具请求通过 SSE 的 `tool_request` 事件出现，不会混入 assistant 文本。

```ts
import { ClientToolError, type JsonValue } from "@agent-hub/client";

client.registerTool("create_ticket", async (input, context): Promise<JsonValue> => {
  const title = typeof input === "object" && input !== null && !Array.isArray(input)
    ? input.title
    : undefined;
  if (typeof title !== "string" || title.trim() === "") {
    throw new ClientToolError("invalid_input", "title is required", false);
  }

  const response = await fetch("/api/tickets", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": context.toolCallId,
    },
    body: JSON.stringify({ title }),
    signal: context.signal,
  });
  if (!response.ok) {
    throw new ClientToolError("ticket_api_failed", "Ticket service failed", true);
  }
  return await response.json();
});
```

外部副作用必须以 `context.toolCallId` 作为第三方服务的持久化幂等键。Hub 和 SDK 对工具结果采用 at-most-once 语义；超时、中断或结果未知时不会自动重放。显式调用 `client.reauthorize()` 可取得最新工具集合，但在途 Run 继续使用创建时的工具快照。

## 匿名浏览器与公开 Widget

匿名模式只能由 `admin` 或 `super_admin` 配置。Integration App 必须关闭登录要求、关联恰好一个 Agent、关闭历史，并配置至少一个精确 Origin。浏览器不提交 secret、用户资料或工具定义：

```ts
import { connectAnonymous } from "@agent-hub/client";

const client = await connectAnonymous({
  baseUrl: "https://hub.example.com",
  clientId: "replace-with-public-client-id",
});

const session = client.currentSession() ?? client.draft();
await session.send("你好", {
  clientMessageKey: `message:${crypto.randomUUID()}`,
});
```

也可以直接嵌入 Hub 托管的 Widget：

```html
<iframe
  src="https://hub.example.com/widget?app=replace-with-public-client-id"
  title="AI assistant"
  width="420"
  height="720"
></iframe>
```

SDK 会用 App-scoped visitor key 恢复唯一的当前匿名 Session。清除浏览器存储后会形成新访客，旧 Session 无法被发现。匿名工具只能来自管理员保存的 Client Tool 定义，并继续受匿名沙箱限制。

## 服务端 API

不需要浏览器 SDK时，第三方后端可以使用 Integration App 的 OAuth `client_credentials` 或 `authorization_code` grant，并调用 `/api/integrations/*`：

```js
const basic = Buffer.from(
  `${process.env.AGENT_HUB_CLIENT_ID}:${process.env.AGENT_HUB_CLIENT_SECRET}`,
).toString("base64");
const form = new URLSearchParams({
  grant_type: "client_credentials",
  scope: `agent:${process.env.AGENT_HUB_AGENT_ID}`,
});

const response = await fetch(`${process.env.AGENT_HUB_URL}/api/oauth/token`, {
  method: "POST",
  headers: {
    Authorization: `Basic ${basic}`,
    "Content-Type": "application/x-www-form-urlencoded",
  },
  body: form,
});
if (!response.ok) throw new Error(`OAuth failed: ${response.status}`);
const token = await response.json();
```

`AGENT_HUB_CLIENT_SECRET` 必须由服务端 secret manager 注入，不得硬编码、写入命令行参数或发送到浏览器。

1. 通过 `POST /api/oauth/token` 获取 Application Token，请求明确的 `agent:<uuid>` scope。
2. 通过 `POST /api/integrations/sessions` 创建外部 Session。
3. 通过 Session 消息接口发送内容，通过 SSE 端点消费有序事件。
4. 使用 Run stop 端点停止当前 Turn。

`client_credentials` 代表应用，创建 Session 时必须提交可信合法邮箱；`authorization_code` 代表已绑定的 Hub User。完整请求字段、响应结构和错误码请使用控制台“API 文档”。

使用 `authorization_code` 时，接入方必须生成不可预测的 `state`，并在回调时自行校验以防止 CSRF。Hub 会原样返回 `state`，但不会代替接入方验证它。

## 生命周期与错误处理

- Client Access Credential 有效期为 15 分钟，SDK 默认提前 60 秒续期；持续授权失败会作为错误暴露，不会无限重试。
- 应用主动重试结果不确定的消息时，必须复用原 `clientMessageKey`，避免重复创建消息或 Run。
- SSE 使用事件 sequence 续传并丢弃重复事件。页面切换时释放不再显示的订阅。
- 同一 Session 同时只有一个活动 Turn；活动期间再次发送会立即引导当前 Turn，而不是创建并行 Turn。
- `401` 通常表示 credential 到期或授权失效；`403` 通常表示 Origin、Agent delegation 或用户权限不匹配；`409` 通常表示 Session/Run 状态冲突。
- 页面卸载、退出登录或账号切换时调用 `subscription.dispose()`、`session.dispose()` 和 `client.dispose()`。

## 上线安全清单

- `client_secret` 和 Application Token 只保存在可信后端及 secret manager。
- 外部用户身份、Tenant、Agent 和工具集合都由服务端从当前登录 Session 与策略推导。
- 不把 bearer Token 写入 Web Storage、URL、DOM、analytics、日志或错误快照。
- 生产环境使用全链路 HTTPS，并配置最小精确 Origin 白名单。HTTP 虽可配置，但会暴露 bearer 窃听风险，也可能被浏览器 mixed-content 策略阻止。
- 校验 Client Tool 输入；高风险动作在第三方 UI 中确认，并在第三方后端再次鉴权。
- 日志过滤 `Authorization`、用户隐私字段和完整工具结果。

## 进一步参考

- [Browser Client SDK 接入指南](./client-sdk-guide.md)：完整安装、React 生命周期、续期、SSE、错误恢复和安全示例。
- [Integration App、Browser SDK 与 Client Tool Spec](./integration-spec.md)：规范契约与验收边界。
- [TypeScript SDK README](../sdk/typescript/README.md)：导出类型、构建、测试和打包。
- `/openapi.json`：当前部署的机器可读 API 定义。
