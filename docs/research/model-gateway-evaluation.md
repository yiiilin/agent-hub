# 多协议模型网关：嵌入式协议引擎评估

## 研究范围与固定版本

- 研究截止日期：2026-07-21。
- 本轮只回答一个问题：怎样为 Agent Hub 提供轻量、尽量无状态的多协议 AI data plane。模型连接、权限、provider endpoint/key、Agent 绑定以及业务 usage/error ledger 继续由 Hub 管理。
- 只使用项目官方文档和官方源码；源码固定到以下版本，避免引用漂移：
  - Bifrost Core：[`core/v1.7.2`，commit `4cfbd369aa0376515438941050bb898bea5e7730`](https://github.com/maximhq/bifrost/tree/4cfbd369aa0376515438941050bb898bea5e7730/core)；HTTP Transport：[`transports/v1.6.4`，commit `c4cd51af26e0e870d4d16d006d1257c08822fd13`](https://github.com/maximhq/bifrost/tree/c4cd51af26e0e870d4d16d006d1257c08822fd13/transports)。为核对 stable tag 之后的公开说明，同时参考固定 commit [`34541a01d97ca499671ac0051e4b7955e3c25ffd`](https://github.com/maximhq/bifrost/tree/34541a01d97ca499671ac0051e4b7955e3c25ffd)。
  - LiteLLM：commit [`c1b6c4062ef5372c1e5e0027c721a942d2bb66cb`](https://github.com/BerriAI/litellm/tree/c1b6c4062ef5372c1e5e0027c721a942d2bb66cb)，稳定版参照 [`v1.93.0`](https://github.com/BerriAI/litellm/releases/tag/v1.93.0)。
  - Portkey AI Gateway：commit [`669825cbe89ee51569918b8f78a9db486fd69dd4`](https://github.com/Portkey-AI/gateway/tree/669825cbe89ee51569918b8f78a9db486fd69dd4)。
  - Helicone AI Gateway：commit [`9649b27bdc9fb0907d359e899894102a15f3a085`](https://github.com/Helicone/ai-gateway/tree/9649b27bdc9fb0907d359e899894102a15f3a085)。

## Control plane 与 protocol/data plane

目标边界如下：

```text
Codex Runtime
  -> Agent Hub (control plane + 请求入口)
       - 鉴权、Global/Personal 权限、Agent 绑定
       - 从数据库读取本次调用的 provider、endpoint、key
       - 保存业务 usage/error 历史
  -> Protocol gateway (data plane)
       - 接收本次调用的 provider、endpoint、key 和 Responses 请求
       - OpenAI Responses <-> 上游协议转换
       - SSE、reasoning、tool calls/tool outputs、usage 透传
       - 不持久化模型连接、key、prompt 或 response
  -> Provider
```

因此，“动态配置”在本轮不是网关 CRUD，而是网关能否在**单次请求**中可靠接收 endpoint/key。将来可以把 key authority 迁入独立网关，但这不是当前实现前提，也不应污染本次选型。

## Embedded-library evaluation

### Bifrost Core 是否是可嵌入 Go library

**结论：是官方支持、带独立版本标签的 Go SDK；可以用它自建 Go 网关。但它不是一个只含纯转换函数的窄 SDK，也没有跨语言稳定 ABI。**

第一方证据：

- 官方 README 明确提供 `go get github.com/maximhq/bifrost/core` 的 “Go SDK / embedded deployment” 入口（[README](https://github.com/maximhq/bifrost/blob/34541a01d97ca499671ac0051e4b7955e3c25ffd/README.md#L155-L173)）。`core` 是独立 Go module（[`core/go.mod`](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/go.mod)），不依赖 `framework`、`transports` 或官方 plugin 实现。
- 嵌入方实现 `Account` 的三个方法：列出 provider、按请求 context 返回 key、按 provider 返回配置（[`Account`](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/schemas/account.go#L767-L783)），再调用 `bifrost.Init`（[`Init`](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/bifrost.go#L218-L380)）。因此薄网关可以用内存 `Account`，不需要 config store。
- Core 暴露 typed `ResponsesRequest` 与 `ResponsesStreamRequest`，流式结果是 Go channel（[public API](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/bifrost.go#L827-L925)）。
- `BifrostConfig` 接收 `Account`、可选 LLM/MCP plugins、logger/tracer、`KeySelector` 等（[schema](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/schemas/bifrost.go#L25-L42)）。这些都是 Go source API，不是 C ABI。

“稳定”的边界需要说清：Core 有正式 tag 和官方 SDK 文档，足以固定版本投入 PoC；但项目迁移文档记录过 Go SDK breaking changes（[v1.5 migration](https://github.com/maximhq/bifrost/blob/34541a01d97ca499671ac0051e4b7955e3c25ffd/docs/migration-guides/v1.5.0.mdx#L608-L680)）。因此必须 pin 精确 tag/commit，并在我们自己的窄 adapter 后使用，不能假设任意 minor upgrade 都源码兼容。

### Core、transports、plugins、config store 的边界

| 部分 | 所有权与作用 | 薄网关是否需要 |
| --- | --- | --- |
| `core` | Provider clients、统一 schemas、key 选择/retry、Responses/provider 转换、stream channel、plugin interfaces | **需要** |
| `transports/bifrost-http` | HTTP 路由、OpenAI/Anthropic wire request 解码、SSE/wire response 编码、管理 API、UI/auth 等服务器装配 | **不应整体引入**；只参考或抽取 Responses wire adapter |
| `plugins/*` | logging、governance、telemetry、semantic cache、compat 等 Core plugin interface 的实现 | 当前全部可省略 |
| `framework/configstore` | SQLite/PostgreSQL-backed provider/key/client/plugin 配置，供完整 Transport 管理面使用 | 当前不需要；Hub 是 control plane |

完整 Transport 自己把 framework account、plugins 与 Core 装配后调用 `bifrost.Init`（[`server.go`](https://github.com/maximhq/bifrost/blob/34541a01d97ca499671ac0051e4b7955e3c25ffd/transports/bifrost-http/server/server.go#L1835-L1861)），且其 module 显式依赖 Core、Framework 和多个 plugin module（[`transports/go.mod`](https://github.com/maximhq/bifrost/blob/34541a01d97ca499671ac0051e4b7955e3c25ffd/transports/go.mod#L1-L35)）。这解释了为什么“嵌 Core 自建薄网关”比 fork 完整 Transport 更符合当前边界。

但 Core public API 是 typed Go API，不是 OpenAI HTTP server。自建薄网关仍需完成三件事：解析 OpenAI Responses wire JSON、调用 Core、把 stream chunk 编码回 typed SSE。官方 Transport 的这些逻辑集中在 OpenAI integration（[Responses request/response converter](https://github.com/maximhq/bifrost/blob/34541a01d97ca499671ac0051e4b7955e3c25ffd/transports/bifrost-http/integrations/openai.go#L273-L402)、[Responses routes/stream converter](https://github.com/maximhq/bifrost/blob/34541a01d97ca499671ac0051e4b7955e3c25ffd/transports/bifrost-http/integrations/openai.go#L697-L750)）。应在固定版本上复用或小范围移植这些 adapter，不能误以为只调用一个转换函数就得到完整 HTTP 网关。

### 请求级 key 与 endpoint：决定性限制

**请求级 raw key：有稳定公开接口。** `core/v1.7.2` 暴露 `BifrostContextKeyDirectKey`；选择路径检测到它后直接使用该 `schemas.Key`，绕过 key pool（[context key](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/schemas/bifrost.go#L218-L230)、[selection path](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/bifrost.go#L8230-L8250)）。官方 Go SDK 文档也明确说明 direct key 无需 gateway flag（[request options](https://github.com/maximhq/bifrost/blob/34541a01d97ca499671ac0051e4b7955e3c25ffd/docs/providers/request-options.mdx#L167-L220)。）Hub 可将解密后的 key 只放入本次进程内 request context，不落 Bifrost store。

**请求级任意 endpoint：没有同等级的稳定公开契约。** `Account.GetConfigForProvider` 不接收 request context；`NetworkConfig.BaseURL` 在 provider 初始化时固化（[`NetworkConfig`](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/schemas/provider.go#L46-L69)、[OpenAI provider construction](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/providers/openai/openai.go#L38-L93)）。源码中的 `BifrostContextKeyURLPath` 对 OpenAI/Anthropic 会接受 absolute URL（[`GetRequestPath`](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/providers/utils/utils.go#L760-L790)），但公开 SDK 文档只把它承诺为“追加到 base URL 的 custom path”（[context-key docs](https://github.com/maximhq/bifrost/blob/34541a01d97ca499671ac0051e4b7955e3c25ffd/docs/quickstart/go-sdk/context-keys.mdx#L129-L136)）。不能把未文档化的 absolute-URL 行为当长期接口。

因此当前准确判断是：

1. key 不需要补丁，也不需要 `KeySelector`；direct-key context 已满足单请求 key。
2. `KeySelector` 只能选择 key，不能改变 endpoint。
3. endpoint 可用“每个 endpoint 一个动态 custom provider”表达，但 provider/client 会留在 Core 内存中，不是真正请求级无状态，且连接数量可能持续增长。
4. 推荐维护一个极小 patch：增加明确的 request-scoped endpoint context/API，并只在受支持 provider 的 URL builder 中读取；patch 必须覆盖普通、SSE 与 Responses lifecycle 路径。不要依赖当前未文档化的 `URLPath=absolute URL` 行为。

### Rust 进程直接嵌 Go Core 的 ABI/cgo/FFI 代价

**不推荐。** Agent Hub 是 Rust，不能像 Go module 一样直接 import Bifrost Core。必须新增 Go shim，再用 Go `-buildmode=c-archive` 或 `c-shared` 导出 C ABI（[Go 官方 build modes](https://pkg.go.dev/cmd/go#hdr-Build_modes)），Rust 再通过 `extern "C"` 调用。

实际成本包括：

- Bifrost 没有现成 C ABI；我们要自行稳定所有 request/response/error/cancel/shutdown 接口。
- Go runtime、GC、goroutine scheduler 仍被带入 Hub 进程；这不是“把几个转换函数静态链接进 Rust”。
- cgo 对 Go pointer 跨边界保存有严格限制（[Go 官方 cgo pointer rules](https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers)）。Responses JSON、错误和尤其持续 SSE stream 需要自定义 buffer ownership、释放函数、取消协议或 callback/poll ABI。
- panic、Rust panic、进程崩溃和 shutdown 互相影响；race detector、profiling、交叉编译、musl/static image 与供应链构建都更复杂。
- 每次 Bifrost SDK schema 变化都要同步 Go shim 与 Rust binding。省掉一次 localhost/Unix-socket hop，换来的是长期双语言 ABI 维护。

如果 Core 最终仍运行在 Go 中，把它做成独立薄进程会保留进程隔离，并且 HTTP/SSE 本来就是现成、可观测、可取消的跨语言边界。

## 同类项目的协议引擎可嵌入性

| 候选 | 可嵌入性结论 | 对当前需求的关键覆盖/缺口 |
| --- | --- | --- |
| **Bifrost Core** | **官方 Go SDK，可嵌 Go；不可直接嵌 Rust。** | Responses typed schemas、SSE、reasoning、tool/function item、usage 和 Anthropic 双向转换证据最强；raw key 可请求级传入；任意 endpoint 需要薄补丁或 provider 实例隔离。 |
| **LiteLLM SDK** | **官方 Python library，可嵌 Python；不可直接嵌 Rust。** `pyproject.toml` 定义 library，公开 `responses/aresponses`（[`pyproject.toml`](https://github.com/BerriAI/litellm/blob/c1b6c4062ef5372c1e5e0027c721a942d2bb66cb/pyproject.toml#L1-L31)、[`responses`](https://github.com/BerriAI/litellm/blob/c1b6c4062ef5372c1e5e0027c721a942d2bb66cb/litellm/responses/main.py#L404-L470)）。 | 每次调用可传 `api_base/api_key`，比 Bifrost 更贴合无状态 endpoint/key；支持 streaming/reasoning/tool/usage。但 Responses -> Chat bridge 明确丢弃无 Chat 等价物的 tools（[`transformation.py`](https://github.com/BerriAI/litellm/blob/c1b6c4062ef5372c1e5e0027c721a942d2bb66cb/litellm/responses/litellm_completion_transformation/transformation.py#L1304-L1321)），而 Python sidecar 更重。 |
| **Portkey AI Gateway** | npm 包可作为 Hono app 嵌入 Node/workerd，但没有独立、稳定的“协议转换引擎”公共包。package 主要入口是 gateway `bin`（[`package.json`](https://github.com/Portkey-AI/gateway/blob/669825cbe89ee51569918b8f78a9db486fd69dd4/package.json)），源码导出完整 Hono app（[`src/index.ts`](https://github.com/Portkey-AI/gateway/blob/669825cbe89ee51569918b8f78a9db486fd69dd4/src/index.ts)）。 | 有 `/v1/responses` routes（[source](https://github.com/Portkey-AI/gateway/blob/669825cbe89ee51569918b8f78a9db486fd69dd4/src/index.ts#L233-L253)），但复用单位是 Node gateway app，不比独立进程更适合 Rust Hub。 |
| **Helicone AI Gateway** | 源码是 Rust workspace，内部模块可被 fork/reuse；但官方标为 Public Beta（[README](https://github.com/Helicone/ai-gateway/blob/9649b27bdc9fb0907d359e899894102a15f3a085/README.md#L1-L20)），当前公开 crate 结构面向完整 gateway（[`Cargo.toml`](https://github.com/Helicone/ai-gateway/blob/9649b27bdc9fb0907d359e899894102a15f3a085/ai-gateway/Cargo.toml)），不是承诺稳定 API 的 Responses conversion crate。 | 同语言是优势，但固定源码的 OpenAI endpoint module 以 Chat Completions 为主（[`endpoints/openai`](https://github.com/Helicone/ai-gateway/tree/9649b27bdc9fb0907d359e899894102a15f3a085/ai-gateway/src/endpoints/openai)）；不能替代已证实的完整 Responses/SSE/reasoning 转换。 |
| **Higress / Kong 类 gateway** | 扩展单位是宿主 gateway 的 plugin/filter，不是可直接嵌入 Agent Hub 的协议库。 | 适合 API gateway policy/traffic 层；没有发现比 Bifrost Core/LiteLLM SDK 更明确的完整 Responses 协议引擎公共 API。本轮停止继续扩展候选。 |

Bifrost 对 Anthropic 的具体覆盖可在固定源码中直接核查：Responses request 双向转换（[`anthropic/responses.go`](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/providers/anthropic/responses.go#L3322-L3537)）、reasoning 与 function/tool item（[同文件](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/providers/anthropic/responses.go#L4248-L4665)）、typed SSE events（[同文件](https://github.com/maximhq/bifrost/blob/4cfbd369aa0376515438941050bb898bea5e7730/core/providers/anthropic/responses.go#L504-L585)）。这些是“实现覆盖”，不是跨 provider 无损等价保证，仍需真实 Codex corpus 验证。

## 四种选择与明确建议

| 选择 | 优点 | 主要代价 | 建议 |
| --- | --- | --- | --- |
| **自建 Rust 网关** | 与 Hub 同技术栈，可共享 Rust types/runtime，无 FFI | 要从头长期维护 Responses、Anthropic、SSE、reasoning、tool、usage 语义；当前成熟 Rust 候选没有同等覆盖 | **暂不选** |
| **在 Rust Hub 内嵌 Bifrost Go library** | 理论上少一个进程/网络 hop | 自建不稳定 C ABI、双 runtime、内存/stream/cancel ownership、构建与崩溃边界复杂 | **明确不选** |
| **自建 Go 薄网关，嵌 Bifrost Core** | 复用最强的协议转换证据；只保留 data plane；Hub 继续管理 endpoint/key；可固定 SDK 版本 | 仍要实现窄 HTTP/SSE adapter；请求级 endpoint 需小 patch；要维护一个 Go 构建产物 | **推荐进入 PoC，并作为当前首选** |
| **直接运行完整 Bifrost Transport 独立进程** | 启动 PoC 最快，官方 HTTP wire adapter 完整 | 带入 config store、plugins、管理面等非必要表面；stock transport 的 direct key 可用，但每请求 endpoint 没有稳定入口 | **只作为兼容性基线，不作为当前最终结构** |

推荐落地形态：

```text
Rust Hub --localhost HTTP/SSE 或 Unix socket--> Go thin-gateway --HTTPS--> Provider
                                                    |
                                                    +-- pinned Bifrost core/v1.7.2
                                                    +-- no DB/config store/plugins
                                                    +-- request-scoped key
                                                    +-- tiny endpoint override patch
```

PoC 只需要证明以下硬门槛：

1. 同一常驻进程连续使用不同 endpoint/key，不串线、不缓存上一请求 secret。
2. OpenAI Responses -> Anthropic Messages 的非流式与 SSE typed events 顺序正确。
3. reasoning summary/encrypted content（上游支持时）、parallel tool calls、`call_id` tool outputs 与 usage 不丢失。
4. 上游 4xx/5xx、429、timeout、客户端取消、流中断能原样形成可判定的 Responses error/terminal 状态。
5. gateway DB 不存在，stdout/stderr 不出现 key、prompt 或 output；请求结束后不保留 secret。
6. 固定 Bifrost commit 的 adapter/patch 有最小回归测试；升级只在同一 corpus 通过后进行。

## 最终判断

可以把 Bifrost 当 Go library 自建网关，这也是当前最有价值的用法；但“嵌入”应发生在一个独立的 Go 薄网关进程中，而不是 Rust Hub 进程内。相较直接运行完整 Bifrost Transport，它的实际价值是去掉重复 control plane、配置数据库、管理 API、UI 与非必要 plugins，只保留协议 data plane。

唯一需要自行承诺的关键扩展是**请求级 endpoint**。请求级 raw key 已有公开接口；endpoint 目前只有静态 provider config 和未文档化的 absolute `URLPath` 源码行为。正式实现应做一个明确、很小、可测试的 Core patch，而不是把未文档化行为当稳定契约。若不接受维护该 patch，次选是 LiteLLM Python SDK sidecar；不建议为省一个本机进程而引入 Rust-Go FFI。

## 实施结果

最终实现固定 `github.com/maximhq/bifrost/core v1.7.2`，但没有 fork 或 patch Core，也没有引入完整 Bifrost Transport。OpenAI Responses 使用标准库 HTTP client 走字节透明 fast path；Anthropic 使用 Core 已导出的 provider Responses build/handle API，把完整 `/v1/messages` URL、request-scoped direct key 和每请求 transport 直接传给 adapter。这样不依赖未文档化的 absolute `URLPath`，也不会把动态 endpoint 累积为常驻 provider 配置。

该实现已用并发不同 endpoint/key、JSON/SSE tool/reasoning 转换、terminal usage、压缩响应字节透明、无 retry、下游取消关闭上游连接、Agent Hub 自有数据的 secret/log redaction 和 Compose 真链测试验证。OpenAI provider body 不做内容扫描，provider 属于连接所有者的信任边界。其版本边界仍是固定 Core API；升级 Bifrost 时必须重新运行 gateway conversion corpus，不能把 minor release 视为自动兼容。
