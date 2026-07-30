# Browser Client SDK 接入指南

本文说明如何从可信后端签发 Client Access Credential，并在浏览器中使用 `@agent-hub/client`。协议约束以 [Integration App、Browser SDK 与 Client Tool Spec](./integration-spec.md) 为准；包入口和构建命令见 [TypeScript SDK README](../sdk/typescript/README.md)。

## 安装

`@agent-hub/client` 当前**没有发布到公共 npm registry**。它是浏览器 ESM 包，带 TypeScript declarations；不提供 Node SDK、React Hooks 包或独立 CDN 文件。

### 同一 workspace 或仓库

把 `sdk/typescript` 作为本地包依赖。当前仓库的 `frontend/package.json` 使用：

```json
{
  "dependencies": {
    "@agent-hub/client": "file:../sdk/typescript"
  }
}
```

路径相对于消费方 `package.json`。随后运行该 workspace 使用的 `npm install` 或 `npm ci`。

### `npm pack` tarball

```bash
cd sdk/typescript
npm ci
npm pack
# 生成 agent-hub-client-0.1.0.tgz

cd /path/to/browser-app
npm install /path/to/agent-hub/sdk/typescript/agent-hub-client-0.1.0.tgz
```

`prepack` 会先构建 `dist`，tarball 只包含包声明允许的文件。

### 内部 registry

本项目没有向任何 registry 发布该包。若组织维护内部 registry，可由发布流程上传已验证的 tarball：

```bash
npm publish ./agent-hub-client-0.1.0.tgz \
  --registry=https://npm.example.internal/
```

消费方在内部版本已发布后安装：

```bash
npm install @agent-hub/client@0.1.0 \
  --registry=https://npm.example.internal/
```

内部发布应固定版本并保留 `npm pack`、测试和制品校验记录，不要把内部 registry 的存在误写成公共 npm 可用性。

## 可信后端签发 Credential

浏览器先由 SDK 生成标签页级 `clientInstanceId`，再把它交给应用的 `authorize` 回调。SDK 在同一标签页刷新时复用该 ID；当 `window.open` 克隆 `sessionStorage` 且原标签页仍存活时，会通过 `BroadcastChannel` 探测冲突并为新标签页重新生成 ID。同一标签页创建的多个 SDK Client 仍共享一个 ID。应用后端必须根据自己的登录 Session 得到可信外部用户资料，并以 HTTP Basic Auth 调用：

```http
POST /api/client/access
Authorization: Basic base64(client_id:client_secret)
Content-Type: application/json
```

`client_secret` 只能存在于可信后端或 secret manager，永不进入浏览器 bundle、HTML、日志、URL 或浏览器请求体。

认证授权中的 `email` 必填，并且必须来自应用后端已经认证的用户资料；`username` 和 `display_name` 只是可选外部资料。Hub 使用可信邮箱关联或创建 Hub User，因此后端不得允许浏览器任意替换该值。

### 完整请求

以下值都是占位示例，不是有效凭证：

```bash
export AGENT_HUB_URL='https://hub.example.com'
export AGENT_HUB_CLIENT_ID='replace-with-client-id'
export AGENT_HUB_CLIENT_SECRET='replace-with-secret-from-secret-manager'

curl --fail-with-body \
  --request POST \
  --user "${AGENT_HUB_CLIENT_ID}:${AGENT_HUB_CLIENT_SECRET}" \
  --header 'Content-Type: application/json' \
  --data '{
    "agent_id": "11111111-1111-4111-8111-111111111111",
    "client_instance_id": "22222222-2222-4222-8222-222222222222",
    "external_user_id": "user-123",
    "tenant_id": "tenant-456",
    "username": "lin",
    "display_name": "Lin",
    "email": "lin@example.invalid",
    "attributes": {
      "plan": "pro"
    },
    "client_tools": [
      {
        "name": "create_ticket",
        "description": "Create a support ticket after user confirmation",
        "input_schema": {
          "type": "object",
          "properties": {
            "title": { "type": "string" }
          },
          "required": ["title"],
          "additionalProperties": false
        }
      }
    ]
  }' \
  "${AGENT_HUB_URL}/api/client/access"
```

`client_tools` 是本次授权的完整动态工具集合。浏览器只能注册对应名称的 handler，不能修改名称、描述或 Schema。后端不得直接相信浏览器提交的用户资料、`agent_id` 或工具定义，应从自己的 Session 和服务端策略推导这些值。

### 完整响应

```json
{
  "access_token": "ahw_<opaque-token-redacted>",
  "expires_at": "2026-07-29T08:15:00Z",
  "expires_in": 900,
  "client_instance_id": "22222222-2222-4222-8222-222222222222",
  "session_id": null,
  "agent": {
    "id": "11111111-1111-4111-8111-111111111111",
    "name": "Support Agent",
    "instructions": "Help the signed-in user."
  },
  "history_enabled": true,
  "tool_names": ["create_ticket"]
}
```

应用后端把该 JSON 返回给发起授权的浏览器即可。服务端伪代码如下；`Buffer` 只运行在服务端，不属于 SDK：

```ts
// POST /api/agent-hub/client-access
async function issueClientAccess(request: AppRequest): Promise<Response> {
  const user = await requireSignedInUser(request);
  const { clientInstanceId } = await request.json();

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
      agent_id: selectAllowedAgent(user),
      client_instance_id: clientInstanceId,
      external_user_id: user.externalId,
      tenant_id: user.tenantId,
      username: user.username,
      display_name: user.displayName,
      email: user.email,
      attributes: trustedAttributes(user),
      client_tools: allowedClientTools(user),
    }),
  });

  return new Response(response.body, {
    status: response.status,
    headers: { "Content-Type": "application/json" },
  });
}
```

## Vanilla TypeScript

下面示例覆盖连接、延迟创建 Session、发送、SSE 订阅、历史列表、恢复已有 Session 和清理。`draft()` 不会立即创建服务端 Session；首条成功消息才会填充 `draft.id`。

```ts
import {
  AgentHubError,
  connect,
  type ClientSession,
  type SessionEvent,
} from "@agent-hub/client";

const client = await connect({
  baseUrl: "https://hub.example.com",
  authorize: async ({ clientInstanceId, signal }) => {
    const response = await fetch("/api/agent-hub/client-access", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ clientInstanceId }),
      credentials: "same-origin",
      signal,
    });
    if (!response.ok) throw new Error(`authorize failed: ${response.status}`);
    return response.json();
  },
});

const draft = client.sessions.draft();
const clientMessageKey = `message:${crypto.randomUUID()}`;
const sent = await draft.send("请总结本周工单", { clientMessageKey });

function onEvent(event: SessionEvent): void {
  if (event.type === "message" || event.type === "assistant") {
    console.log(event.content);
  } else if (event.type === "error") {
    console.error(event.code, event.message, event.retryable);
  }
}

// 订阅从 sequence 0 恢复持久化事件；首条消息后 draft 已成为现有 Session。
const subscription = draft.subscribe(onEvent, { after: 0 });
console.log(sent.sessionId, draft.id);

let existingSession: ClientSession | undefined;
if (client.historyEnabled) {
  const sessions = await client.sessions.list({ limit: 20 });
  if (sessions[0]) {
    existingSession = client.sessions.existing(sessions[0].id);
    const messages = await existingSession.messages({ limit: 50 });
    console.log(messages);
  }
}

// API 错误可按类型处理。
try {
  await client.sessions.list({ limit: 20 });
} catch (error) {
  if (error instanceof AgentHubError) {
    console.error(error.status, error.code, error.details);
  }
}

// 页面卸载、视图切换或退出登录时清理。
subscription.dispose();
existingSession?.dispose();
draft.dispose();
client.dispose();
```

不要在清理后继续使用 `client` 或 `ClientSession`。

### 稳定消息 key

SDK 会为一次 `send()` 自动生成 `client_message_key`，并在该次内部网络重试中复用。若应用在请求结果不确定后自行重试，必须预先生成并再次传入同一个 `clientMessageKey`：

```ts
const key = `message:${crypto.randomUUID()}`;

try {
  await session.send(text, { clientMessageKey: key });
} catch (error) {
  // 应用判断交付结果确实不确定后，向用户提供一次使用原 key 的重试操作。
  offerRetry(() => session.send(text, { clientMessageKey: key }), error);
}
```

`offerRetry` 代表应用自己的交互和错误分类，不是 SDK API。参数校验、`401`、`403` 等确定失败不应重试。

## React 生命周期

SDK 没有 React Hooks 包。React 应用直接在 `useEffect` 中创建客户端，并在 cleanup 中释放订阅和客户端：

```tsx
import { useEffect, useState } from "react";
import {
  connect,
  type AgentHubClient,
  type SessionEvent,
  type SessionSubscription,
} from "@agent-hub/client";

export function Conversation() {
  const [events, setEvents] = useState<SessionEvent[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    let client: AgentHubClient | undefined;
    let subscription: SessionSubscription | undefined;

    void (async () => {
      const nextClient = await connect({
        baseUrl: "https://hub.example.com",
        authorize: async ({ clientInstanceId, signal }) => {
          const response = await fetch("/api/agent-hub/client-access", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ clientInstanceId }),
            signal,
          });
          if (!response.ok) throw new Error(`authorize failed: ${response.status}`);
          return response.json();
        },
      });

      if (cancelled) {
        nextClient.dispose();
        return;
      }
      client = nextClient;

      const session = client.sessions.draft();
      await session.send("你好", {
        clientMessageKey: `message:${crypto.randomUUID()}`,
      });
      if (cancelled) return;

      subscription = session.subscribe((event) => {
        setEvents((current) => [...current, event]);
      });
    })().catch((cause: unknown) => {
      if (!cancelled) {
        setError(cause instanceof Error ? cause.message : "Agent Hub connection failed");
      }
    });

    return () => {
      cancelled = true;
      subscription?.dispose();
      client?.dispose();
    };
  }, []);

  if (error) return <p role="alert">{error}</p>;
  return <ol>{events.map((event) => <li key={event.sequence}>{event.type}</li>)}</ol>;
}
```

开发模式下 React Strict Mode 可能执行一次额外的 setup/cleanup；cleanup 必须始终可重复调用。不要把 `AgentHubClient` 留在已卸载组件的异步闭包中。

## 匿名接入与恢复

匿名 Integration App 由管理员预先固定一个 Agent、关闭 history，并配置 Client Tools 与至少一个 Origin。浏览器只需要公开 `clientId`：

```ts
import { AgentHubError, connectAnonymous } from "@agent-hub/client";

const client = await connectAnonymous({
  baseUrl: "https://hub.example.com",
  clientId: "public-client-id",
});

const session = client.currentSession() ?? client.draft();
if (session.isDraft) {
  await session.send("开始匿名会话", {
    clientMessageKey: `message:${crypto.randomUUID()}`,
  });
} else {
  console.log(await session.messages({ limit: 50 }));
}

try {
  await client.listSessions();
} catch (error) {
  if (error instanceof AgentHubError) {
    console.log(error.status, error.code); // 403, anonymous_history_disabled
  }
}
```

SDK 在 `localStorage` 保存 App-scoped visitor key 和唯一的当前 `session_id`，在 `sessionStorage` 保存标签页级 Client Instance ID。Client Access Credential 始终只在内存中。刷新后 `connectAnonymous()` 会携带 visitor key 和精确 `session_id` 恢复当前 Session；匿名客户端不能调用历史列表、发现其他 Session 或恢复其他 visitor 的 Session。清除本地存储后会形成新 visitor，旧 Session 不可发现。

匿名浏览器不能提交 Client Tool 定义，只能为管理员配置且已授权的工具名称注册 handler。

## Client Tool handler

`registerTool()` 只能注册当前 Credential 的 `authorizedToolNames`。SDK 按模型输出顺序串行处理工具调用；应用抛出 `ClientToolError` 可返回结构化、无 JavaScript stack 的失败结果：

```ts
import { ClientToolError, type JsonValue } from "@agent-hub/client";

client.registerTool("create_ticket", async (input, context): Promise<JsonValue> => {
  const title = typeof input === "object" && input !== null && !Array.isArray(input)
    ? input.title
    : undefined;
  if (typeof title !== "string" || title.trim() === "") {
    throw new ClientToolError("invalid_input", "title is required", false);
  }

  if (!window.confirm(`Create ticket: ${title}?`)) {
    throw new ClientToolError("user_rejected", "User rejected ticket creation", false);
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

  const ticket = await response.json() as { id: string };
  return { ticket_id: ticket.id };
});
```

外部写操作必须以 `context.toolCallId` 作为应用边界的幂等键，并在外部服务端对该 key 建唯一约束、持久化首次结果、重复请求返回同一结果。Hub 的 claim/result 幂等不能保证外部系统 exactly-once。handler 执行中断、超时或提交结果未知时，SDK 将 journal 标为 `unknown`，停止该模型批次的剩余 Client Tools，不会自动重放副作用，也不会让其他标签页接管。

handler 可直接返回 JSON 值或完整 `ToolResult`。结果 JSON 的 UTF-8 编码上限为 16,000 字节；SDK 不截断，也不自动重试 `ClientToolError`。`retryable` 是传给模型和 UI 的错误属性，不是自动重放指令。

## 凭证续期、SSE 与显式重新授权

- Client Access Credential 是 15 分钟 opaque bearer Token。SDK 默认在到期前 60 秒调用 `POST /api/client/renew`，续期沿用原工具 Grant，并原子替换当前标签页的 Token。
- `renew` 返回 `401` 时，认证模式对该次恢复只调用一次应用 `authorize` callback；匿名模式重新调用匿名 access。持续失败会作为 typed error 暴露，不会无限授权重试。
- `await client.reauthorize()` 会立即重新执行认证 `authorize`，或重新获取匿名 Credential，用于取得最新工具集合。已在途 Run 仍使用创建时的 Tool Snapshot。
- `subscribe(listener, { after })` 以服务端 event `sequence` 为 cursor。断线重连携带 query cursor 和 `Last-Event-ID`，丢弃不大于当前 cursor 的重复事件；调用方保存最后处理的 `sequence` 后可显式恢复。
- `tool_request`、`tool_result`、`timeout` 和 `error` 是 `SessionEvent` 的 typed 分支，不会混入 assistant 文本。
- 工具执行进入 `unknown` 后绝不自动重放。5 分钟硬期限、执行中断或结果未知会停止该批剩余调用，并使 Turn 失败。

显式重新授权示例：

```ts
await client.reauthorize();
console.log([...client.authorizedToolNames]);
```

重新授权后，已注册但不再授权的 handler 不会获得新调用；新授权名称仍需调用 `registerTool()` 或 `registerTools()` 注册 handler。

## Origin 与 HTTP 风险

Origin 使用 `scheme://host[:port]` 规范化后精确匹配。只允许 `http` 或 `https`，不允许 wildcard、用户名密码、path、query 或 fragment；scheme、host 或非默认 port 不同均是不同 Origin。

| 模式 | Origin 配置 | 请求规则 |
| --- | --- | --- |
| 认证应用 | 可为空 | 空列表允许任意浏览器 Origin；配置后，浏览器请求必须携带并匹配。可信后端的 Basic Auth 签发请求可不带 `Origin`。 |
| 匿名应用 | 必填且至少一个 | access、续期、Session、SSE 和工具请求都必须携带匹配的浏览器 Origin。 |

外部页面直接使用 SDK 时，Hub 校验浏览器原生 `Origin`，应用不要自行设置 `X-Agent-Hub-Embedded-Origin`。只有 Hub 托管的 iframe Widget 需要转发宿主 Origin：认证 Widget 采用已绑定的 `postMessage` `event.origin`；匿名 Widget 首次加载优先采用 `ancestorOrigins[0]`，再回退到 `document.referrer`。Widget 对 Hub 的同源请求携带内部 `X-Agent-Hub-Embedded-Origin`，Hub 仅在 `Sec-Fetch-Site: same-origin` 时接受该值。反向代理必须保留这两个 header；`Sec-Fetch-Site` 约束用于阻止浏览器跨站脚本伪造内部 header，不能替代 Credential、Agent delegation 或服务端身份校验。

平台允许在生产配置 HTTP Origin，但这不代表 HTTP 安全：任何链路监听者都可能窃取 bearer Token，并在最长 15 分钟窗口内冒用。HTTPS 页面访问 HTTP Hub 还会被浏览器按 mixed content 阻止。生产环境应让应用页面、授权后端和 Hub 全链路使用 HTTPS，并确认反向代理不会降级或记录 Authorization header。

## 错误恢复与清理

请求失败会抛出 `AgentHubError`，可读取 `status`、`code` 和 `details`。事件流错误通过 `ErrorSessionEvent` 提供 `code`、`message`、`retryable`。建议按以下边界恢复：

- 消息 POST 结果不确定：复用同一个 `clientMessageKey`；不要用新 key 盲目重发。
- SSE 网络断开或服务端 `5xx`：SDK 使用 cursor 自动重连；`4xx` 会关闭当前循环，应修复权限或 Origin，必要时 `reauthorize()` 后新建订阅。
- `401`：SDK 自动尝试一次 renew/authorize 恢复；若仍抛错，由应用重新登录或显示失败状态。
- handler 普通失败：返回或抛出 `ClientToolError`；不要在 SDK 外再次执行同一个副作用。
- handler timeout、abort 或 `unknown`：不要重放，不要跨标签页接管；等待 Hub 的终态并向用户显示该 Turn 失败。
- 视图切换：释放不再显示的 `SessionSubscription`；不需要继续使用该 Session 时调用 `session.dispose()`。
- 页面卸载或退出登录：调用 `client.dispose()`。这会停止续期、SSE 和在途请求并清除内存 Token，但服务端旧 Token 仍可能存活到到期，最长 15 分钟。

## 安全清单

- `client_secret` 仅保存在可信后端；浏览器只接收短期 `access_token`。
- 后端从登录 Session 推导 external identity、tenant、Agent 和 Client Tools，不接受浏览器扩权。
- 不把 bearer Token 写入 Web Storage、IndexedDB、URL、日志、analytics、错误快照或 DOM；SDK 默认只保存在内存。
- 生产全链路使用 HTTPS，并配置最小精确 Origin 白名单。
- 校验每个 handler 的输入；高风险操作由应用提供确认 UI 和服务端授权检查。
- 外部副作用使用 `context.toolCallId` 做持久化幂等，不把 `retryable` 理解为自动重放许可。
- 不记录 handler stack、敏感输入或完整工具结果；服务端日志过滤 `Authorization`。
- 对组件卸载、路由切换、退出登录和账号切换执行 `subscription.dispose()`、`session.dispose()`、`client.dispose()`。
