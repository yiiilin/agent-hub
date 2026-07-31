# `@agent-hub/client`

Agent Hub 的框架无关 Browser SDK。包提供 ESM 和 TypeScript declarations，只依赖浏览器标准 API。

当前版本是 `0.1.0`，**未发布到公共 npm registry**。V1 不提供 React Hooks、Node SDK 或独立 CDN 文件。完整授权、Session、匿名恢复、Client Tool、Origin 与安全说明见 [Browser Client SDK 接入指南](../../docs/client-sdk-guide.md)，协议约束见 [integration spec](../../docs/integration-spec.md)。

## 安装

同一仓库或 workspace 使用本地依赖；路径相对于消费方 `package.json`：

```json
{
  "dependencies": {
    "@agent-hub/client": "file:../sdk/typescript"
  }
}
```

tarball 安装：

```bash
cd sdk/typescript
npm ci
npm pack

cd /path/to/browser-app
npm install /path/to/sdk/typescript/agent-hub-client-0.1.0.tgz
```

组织也可以把已验证 tarball 发布到内部 registry，再安装明确版本：

```bash
npm publish ./agent-hub-client-0.1.0.tgz --registry=https://npm.example.internal/
npm install @agent-hub/client@0.1.0 --registry=https://npm.example.internal/
```

这些命令不表示包已经存在于公共或内部 registry；内部发布由消费组织负责。

## 快速开始

`authorize` 调用应用自己的可信后端。可信后端再用 `client_id`/`client_secret` 和 Basic Auth 调用 Hub 的 `POST /api/client/access`；`client_secret` 永不进入浏览器。

```ts
import { connect, type SessionEvent } from "@agent-hub/client";

const client = await connect({
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

const session = client.sessions.draft();
await session.send("你好", {
  clientMessageKey: `message:${crypto.randomUUID()}`,
});

const subscription = session.subscribe((event: SessionEvent) => {
  console.log(event.type, event.sequence);
});

subscription.dispose();
client.dispose();
```

匿名应用使用管理员预先配置的公开 `clientId`：

```ts
import { connectAnonymous } from "@agent-hub/client";

const client = await connectAnonymous({
  baseUrl: "https://hub.example.com",
  clientId: "public-client-id",
});

const session = client.currentSession() ?? client.draft();
```

匿名模式保存 App-scoped visitor key 和唯一当前 Session 以支持刷新恢复，但不允许列出历史 Session。Client Access Credential 在认证和匿名模式下都只保存在内存。

## Public API

主要 value exports：

- `connect()`、`connectAnonymous()`
- `AgentHubClient`
- `ClientSession`，以及别名 `Session`
- `SessionSubscription`
- `AgentHubError`、`ClientToolError`
- `IndexedDbToolJournalStorage`、`MemoryToolJournalStorage`

`AgentHubClient` 提供：

- `sessions.list()`、`sessions.existing()`、`sessions.draft()`
- 同义方法 `listSessions()`、`existing()`、`draft()`
- `currentSession()`、`registerTool()`、`registerTools()`、`unregisterTool()`
- `reauthorize()`、`stop()`、`dispose()`
- `clientInstanceId`、`authorizedToolNames`、`agent`、`historyEnabled`、`isAnonymous`

`ClientSession` 提供 `id`、`isDraft`、`messages()`、`messagePage()`、`events()`、`send()`、`stop()`、`subscribe()` 和 `dispose()`。`events()` 读取当前 Session 的持久事件，`subscribe()` 继续接收后续事件并返回 `SessionSubscription`；订阅可调用 `dispose()` 或 `unsubscribe()`，也可等待其 `closed` Promise。

包还导出 `ClientCredential`、`SessionEvent`、`ToolHandlerContext`、`ToolResult` 等 public types；以 [`src/index.ts`](./src/index.ts) 为完整导出清单。

## 关键行为

- Credential 有效期 15 分钟；SDK 默认提前 60 秒续期。renew `401` 时，认证模式只进行一次 `authorize` fallback，匿名模式重新获取 anonymous access。
- Client Instance 在标签页刷新后保持稳定；`BroadcastChannel` 会检测 `window.open` 克隆的活跃 ID 并为新标签页重新生成 ID，同一标签页中的多个 Client 共享 ID。
- `reauthorize()` 获取最新 Grant；在途 Run 仍使用原 Tool Snapshot。
- `send()` 支持稳定 `clientMessageKey`；应用级不确定重试必须复用同一个 key。
- `subscribe()` 使用 SSE event sequence cursor 断线续传，并输出 typed `tool_request`、`tool_result`、`timeout`、`error` 事件。
- Client Tool handler 串行执行。外部副作用必须使用 `context.toolCallId` 做服务端幂等。
- `ClientToolError` 生成结构化错误；handler 已执行但结果提交状态未知、timeout、abort 或 `unknown` 调用不会自动重放，并会阻断同批剩余工具。
- React 直接使用 `useEffect` 管理 `connect()`、订阅和 `dispose()` 生命周期；本包没有 React Hooks。
- Origin 按 `scheme://host[:port]` 精确匹配。认证 App 可不配置白名单，匿名 App 必须配置；外部 SDK 依赖浏览器原生 `Origin`，不得设置 Widget 内部使用的 `X-Agent-Hub-Embedded-Origin`。生产 HTTP 会暴露 bearer 窃听风险，HTTPS 页面访问 HTTP Hub 还会被 mixed content 阻止。

## 构建与验证

```bash
npm ci
npm run build
npm test
npm pack
```

`npm test` 会先构建，再运行 Node test runner 中的 SDK 测试；SDK 运行时目标仍是浏览器。
