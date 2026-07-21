# Model Connections and Token Usage Spec

## 范围

1. Model Connection 是独立的模型连接配置，包含显示名称、服务根地址、Model ID、Upstream Protocol、加密 API Key、启停状态和 Global/Personal 作用域。
2. Upstream Protocol 只允许 `openai_responses` 和 `anthropic_messages`。省略该字段的旧请求和升级前已有记录默认为 `openai_responses`；未知值必须由 API 和数据库拒绝。
3. 服务根地址不包含 `/v1`，但可以带业务路径。`openai_responses` 使用 Responses 字节透明路径；`anthropic_messages` 由内部协议网关转换。Runtime 始终使用原有 Responses 契约，第一版不提供 Chat Completions 入口。
4. Personal Model Connection 仅能分配给同一 owner 的 Agent；Global Model Connection 可分配给所有 Agent。共享或公开 Agent 始终使用 Agent owner 配置的连接，调用者不能替换。
5. Administrator 管理 Global Model Connection；普通用户管理自己的 Personal Model Connection。`admin` 不得查看或修改 `super_admin` 的个人连接，但可管理不属于个人的 Global Model Connection。
6. 管理员可选择一个 System Default Model Connection。创建 Agent 时复制该 Global 连接；之后修改系统默认不影响已有 Agent。没有系统默认时仍可创建 model-unconfigured Agent，但不能开始新 Turn。
7. 每个 Agent 选择一个默认连接和 reasoning effort。每个 Codex Subagent Definition 包含唯一名称、用途描述、Markdown developer instructions，以及可选的连接和 reasoning override；省略时继承 Agent 默认值。
8. reasoning effort 支持模型默认、`none`、`minimal`、`low`、`medium`、`high`、`xhigh`、`max` 和 `ultra`。Hub 不静默降级供应商拒绝的取值。
9. 子 Agent 共用主 Agent 的 Workspace、Skills、MCP 和 sandbox authority，不创建独立 Hub Session，也不能扩大权限。第一版使用 Codex 默认的最多 6 个并发线程和一层子 Agent 深度，不提供调节项。

## 连接和密钥生命周期

- API Key 在创建 Model Connection 时必填。更新请求不提交新明文 Key 时保留旧值；查询、列表、错误、日志和 Runtime 协议永不返回明文。
- Hub 从 `HUB_MODEL_SECRET_KEY` 读取一把部署级对称主密钥，使用带完整性校验的对称加密和每条 secret 独立随机 nonce 保存模型 API Key。第一版不支持主密钥轮换；缺失或格式错误时 Hub 拒绝启动。
- Model Connection 可停用。已经建立的流继续完成；下一次模型请求拒绝使用。重新启用保留所有引用并恢复可用性。
- 普通删除在仍被 System Default、Agent 默认或子 Agent override 引用时返回冲突。Force Delete 删除密钥和可执行配置并保留非 secret 历史快照：Agent 默认连接被删除后 Agent model-unconfigured；子 Agent override 被删除后只禁用该定义，不静默恢复继承。
- 删除或停用 System Default 时清除默认选择。后续新 Agent 保持 model-unconfigured，已有 Agent 不自动改绑。
- Base URL 或 API Key 修改从下一次模型请求生效，已建立的流继续使用请求开始时取得的值。Model ID、Upstream Protocol、reasoning effort 和子 Agent 文件从下一 Turn 生效。
- 新建和编辑不强制访问上游。显式“测试连接”发送最小 Responses 请求；返回的 usage 归入执行测试的 Hub User，不归入 Agent，错误进入 Model Call Error ledger。

## Hub 与 Model Gateway 调用链

1. Runtime 系统流量仍只访问 Hub，不保留或接收真实 provider API Key，也不存在 `RUNTIME_DIRECT_MODEL_*` fallback。
2. Runtime 为 Agent 默认连接和子 Agent override 生成受控 `CODEX_HOME` provider/agent 配置，通过本地 loopback proxy 携带 run-scoped token、Run ID 和所选 Model Connection ID 请求 Hub。
3. Hub 每次模型请求只查询一次数据库，验证 Run、Runtime、Agent、Model Connection scope、启停状态和请求 `model` 与连接 Model ID 一致，再解密该次请求使用的 API Key。
4. Hub 删除 Runtime/Hub authentication、Cookie、Host、Content-Length 和 hop-by-hop headers，把协议、服务根地址、解密后的请求级 API Key、query、安全 headers 和原始 Responses body 发送给只在内部网络开放的 Model Gateway。
5. Gateway 对 `openai_responses` 使用字节透明 fast path，对 `anthropic_messages` 使用固定 Bifrost Core 转换为 Responses JSON/SSE。Hub 流式返回 Gateway status、允许的 response headers 和 body，并旁路解析 terminal response 记录 usage/error，不得把整个响应缓冲后再发送。
6. 成功完成却没有 `usage` 属于 provider protocol error。任何状态只要返回了有效 `usage` 都写入 Model Token Usage；failed、incomplete、cancelled 或 transport failure 同时写入 Model Call Error，只有没有 usage 的失败才不增加 Token 总量。
7. Personal 和 Global Base URL 都不做公网、内网、loopback、link-local、metadata、DNS、redirect 或明文 HTTP 限制，风险由 ADR-0024 明确接受。HTTP client 仍只实现 HTTP/HTTPS 协议，并对 HTTPS 执行标准证书和 hostname 校验，不提供跳过校验选项。连接所有者必须信任所选 provider 接收 API Key 并生成 response body；OpenAI 字节透明路径不扫描 provider body。
8. Gateway 不保存 Model Connection、API Key、prompt、output、usage 或 error，不做 retry/fallback；Hub 仍是唯一业务 control plane 和历史账本权威。协议和传输细节见 `docs/model-proxy-spec.md` 与 ADR-0028。

## 用量和错误账本

- 每个真实上游 Responses 请求最多写入一条 Model Token Usage。一个 Turn 中的多次调用、tool continuation、子 Agent 调用和 provider retry 按实际请求分别记录并汇总。
- usage 保存 `input_tokens`、`output_tokens`、`total_tokens`、`cached_tokens` 和 `reasoning_tokens`；缓存 Token 是输入子集，推理 Token 是输出子集，不重复累加到 total。
- usage 归属于 Hub Agent、发起 subject、Model Connection 和调用时的非 secret 快照，包括调用时固定的 Upstream Protocol，但不区分主 Agent 与具体子 Agent。调用者使用共享 Agent 时计入调用者而不是 Agent owner；Automation 计入 owner；user-level Application Token 计入 represented Hub User；app-only token 计入 Integration App。
- Model Token Usage 和 Model Call Error 均使用 PostgreSQL `TIMESTAMPTZ(3)` 保存毫秒精度发生时间。Error 还保存上游/transport 状态、脱敏错误码与有限长度消息，以及同一权限范围需要的 User、Agent 和 Model 快照；不得保存 prompt、output、raw body、headers 或 credentials。
- 两个 ledger 都不可删减。删除 User 后去除身份关联并显示“已删除用户”；删除 Agent 或 Model Connection 后保留非 secret 历史快照；删除 Session 或 Run 不改变历史总量。错误与用量使用相同生命周期。
- 普通用户可查看自己的 usage/error 和自有 Agent 汇总，但不能看到调用自有共享 Agent 的其他用户身份。`admin` 可查看非 `super_admin` 范围；`super_admin` 可查看全平台。

## API 和管理台

- 新增一级“模型”菜单，包含“我的模型”“可用模型”“用量统计”；Administrator 额外看到“全局模型”。连接表格显示 Upstream Protocol，新建和编辑 FormDialog 必须提供协议选择。测试、启停、普通删除和 Force Delete 同样从表格操作打开 FormDialog，不在主页常驻表单。
- Agent 新建/编辑表单选择默认 Model Connection 和 reasoning effort，并用列表加子表单维护 Codex Subagent Definitions。连接 CRUD 不嵌入 Agent 表单。
- 用量页默认查询“当天”，可选“昨天”“7 天”“30 天”“90 天”“总共”。前端按浏览器本地日历计算毫秒级半开区间 `[from_ms, to_ms)`：当天从本地零点到当前时刻，昨天为完整前一日，7/30/90 天从对应本地零点到当前时刻，总共不传边界；后端只按传入时间戳查询。
- 汇总查询计算整个时间范围的 overall totals，并按 Model、Agent 和 Hub User 三个维度分组，不提供 Session、Run 或 Turn 下钻。汇总不受明细分页影响；usage 和 error 明细分别按发生时间及 ID 倒序分页。
- usage 列表每行是一条带 usage 的 Responses 调用，显示时间、subject、Agent、Model、input/output/cached/reasoning/total。Model Call Error 使用独立分页列表，不混入 usage。
- 第一版不计算费用，不做图表、Token quota、rate limiting 或 CSV export。

## 兼容和非目标

- 不自动迁移 `HUB_MODEL_PROXY_UPSTREAM_URL`、`HUB_MODEL_PROXY_API_KEY`、Agent JSON model policy 或 Runtime direct-model 配置。升级后由 Administrator 创建 Global Model Connection 并明确分配；现有 Agent 在此之前 model-unconfigured。
- Compose 测试环境直接创建开发用 Global Model Connection 和 API Key，并设置开发专用 `HUB_MODEL_SECRET_KEY`；fake provider 同时提供确定性的 OpenAI Responses 与 Anthropic Messages JSON/SSE。
- 不支持 arbitrary connection headers、Chat Completions 入口、provider price catalog、成本换算、secret 查询、主密钥轮换或 Runtime 直连 provider。

## 验收标准

- Global/Personal scope、Administrator/Super Administrator 边界、System Default copy、共享 Agent owner-connection 规则和 model-unconfigured 阻断均有数据库与 API 测试。
- API Key ciphertext 使用随机 nonce，同一明文不会产生相同 ciphertext；任何 read DTO、OpenAPI、日志和 Runtime claim 都不含明文。
- 省略 Upstream Protocol 的旧创建请求和已有数据库记录保持 `openai_responses`；创建、读取、编辑、Agent execution snapshot 和历史 ledger 均保留明确协议，未知协议 fail closed。
- 网关测试证明 OpenAI 原始 JSON bytes、query、允许 headers、status 和 SSE chunks 被流式保留，Anthropic JSON/SSE 被规范化为 Responses，认证按协议替换，每次请求只解析一次连接配置，Runtime 不持有 provider key。
- usage/error 测试覆盖 completed/failed/incomplete/cancelled、带或不带 usage、缓存/推理子集、retry、多请求 Turn、连接测试、共享 Agent attribution、权限过滤和所有删除匿名化路径。
- Runtime 测试覆盖多 provider config、子 Agent 文件、下一 Turn refresh、reasoning values、连接禁用/删除和彻底移除 direct fallback。
- 管理台在桌面与 390px 下验证四个 Model Tabs、CRUD dialogs、Agent model/subagent 配置、固定时间范围 totals、分页 usage/error 和角色可见性，并检查 console/network 与横向溢出。
