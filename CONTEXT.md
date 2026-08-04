# Agent Hub

Agent Hub coordinates isolated agent sessions across external platforms and runtime nodes.

## Language

**Runtime**:
A registered worker node that owns isolated local Session execution directories and runs the Execution Engine. Ordinary deletion first drains its Sessions safely; force deletion declares the node permanently lost and prevents it from participating again under its former identity.
_Avoid_: Agent, Session, Execution Engine process

**Execution Engine**:
The Runtime-local program that executes one Session's ordered Turns against its Workspace and Native Session. Pi is the only Execution Engine implementation in Agent Hub V1.
_Avoid_: Agent, Runtime, model provider

**Pi**:
The pinned standalone Execution Engine distributed as part of the Runtime image. Pi implementation names stay Pi-specific; control-plane contracts use Execution Engine or Native Session terminology.
_Avoid_: Agent, Codex, generic runtime

**Runtime Engine Version**:
The immutable Pi artifact version built into and reported by one Runtime image. Runtime Engine upgrades replace the complete drained Runtime image rather than downloading a binary through Hub.
_Avoid_: Agent version, model version, global rollout version

**Draining Runtime**:
A Runtime that accepts no new Session ownership while every Session it already owns is moved to a current Bundle and releases that ownership. It cannot be ordinarily deleted until no Session remains assigned to it.
_Avoid_: Offline Runtime, deleted Runtime, Recovery-Failed Session

**Runtime Enrollment Token**:
A one-time secret created by a Super Administrator that authorizes exactly one new Runtime identity for thirty minutes. Once consumed, expired, or revoked it cannot register a Runtime; a deleted Runtime needs a newly issued token before it can join again.
_Avoid_: Shared registration token, Runtime credential, reusable API key

**Runtime Credential**:
The long-lived, revocable secret held by exactly one enrolled Runtime and used to authenticate that Runtime across process restarts. It is distinct from the one-time token used to create the Runtime identity.
_Avoid_: Runtime Enrollment Token, shared registration token, Session secret

**External Identity**:
A uniquely identified user identity asserted by one External Platform within its own tenant or account namespace. Its binding to at most one Hub User is stable across external profile changes, while a Hub User may bind multiple External Identities.
_Avoid_: Bare external user ID, globally unique external user

**External Platform**:
An external system integrated with Agent Hub and identified by a stable Hub-wide identifier. A platform is distinct from its OAuth client, tenant, installation, and individual users.
_Avoid_: Integration App, tenant, integration session

**External Tenant**:
A uniquely identified account, workspace, organization, or tenant within one External Platform. External user identifiers are interpreted inside this namespace even when a provider claims broader uniqueness.
_Avoid_: External Platform, Integration App, Hub owner

**Hub User**:
The Agent Hub identity that owns user-scoped sessions, credentials, and stored workspace data. It has an immutable internal ID, one current globally unique email used for Hub login and trusted email binding, and a non-unique Display Name.
_Avoid_: Agent owner when referring to the user participating in a session

**Display Name**:
The non-unique, user-facing name of one Hub User. It is profile data rather than a login identifier and may be changed without changing the Hub User's identity.
_Avoid_: Email, external username, user ID

**Super Administrator**:
The Hub User with unrestricted platform-wide authority, including authority over Administrators, other Super Administrators, and their protected accounts and personally owned resources. The first Hub User created in an empty Agent Hub becomes its initial Super Administrator.
_Avoid_: Agent owner, ordinary administrator, first external user

**Administrator**:
A Hub User that may manage system-wide resources and every non-Super-Administrator account and its data. It cannot inspect, modify, or delete a Super Administrator account, credentials, or personally owned resources; system-wide resources remain administrable regardless of who created them.
_Avoid_: Super Administrator, Agent owner, read-only auditor

**Authentication Channel**:
An administrator-approved identity source belonging to one External Platform and used by Integration Apps to bind trusted External Identities to Hub Users. It is not a Hub console login method.
_Avoid_: LDAP Directory, Local Password Login, Integration App, Session Origin

**Hub Login Method**:
One of the two ways a person may authenticate to the Hub console: Local Password Login or the single global LDAP Directory. Integration App Authentication Channels are not Hub Login Methods.
_Avoid_: Authentication Channel, Application Token, Client Access Credential

**Local Password Login**:
A Hub Login Method that verifies a password stored for a Hub User's email. A Super Administrator with a local password retains a separate emergency path even when ordinary Local Password Login is disabled.
_Avoid_: LDAP Login, password registration, API Key

**Password Registration**:
The public creation of a Hub User with an email and local password. It is enabled for an empty Hub, turns off after the first Hub User is created, and may later be explicitly reopened by an Administrator despite the documented email pre-registration risk.
_Avoid_: Local Password Login, administrator-provisioned account, LDAP Login

**LDAP Directory**:
The single administrator-configured directory against which Hub Users may authenticate with their email and LDAP password. Agent Hub maps the submitted email through one Bind identity template, uses the directory-returned email as the Hub identity, and deliberately keeps no permanent LDAP user identifier or historical email alias.
_Avoid_: Authentication Channel, External Platform, LDAP user database

**Integration App**:
An application registered under one External Platform and Authentication Channel that may be delegated access to multiple Agents. It is the common integration boundary for OAuth clients, external Session APIs, and embedded Widgets.
_Avoid_: Agent, External Platform, Authentication Channel

**Application Token**:
A short-lived credential issued to an Integration App. It represents either the application itself or one authenticated Hub User, and its authority is limited by its Agent Scopes and the application's current Agent delegations.
_Avoid_: API Key, browser Session, Runtime Credential

**Client Access Credential**:
A short-lived opaque credential that an Integration App backend obtains for one delegated Agent and one trusted External Identity so its browser client, including a Widget or headless SDK, can use that application's matching Sessions for the delegated Agent. Its random token remains a compact lookup secret: Hub stores the authorized Client Tool definitions as JSON on the existing persisted credential record rather than embedding schemas in the token or creating a separate tool-grant table. Hub restarts therefore do not invalidate it. It expires after 15 minutes; the SDK renews it before expiry through an atomic same-Client-Instance rotation that immediately supersedes the old token while leaving in-flight Runs intact and allowing their tool results to use the replacement token. V1 has no separate manual-revocation endpoint: expiry, rotation, and live application/user/Agent status checks invalidate access. It does not expose application authority and does not itself create or identify a conversation Session.
_Avoid_: Widget Access Credential, Integration App client secret, Application Token, permanent frontend token, self-contained JWT, tool schemas embedded in a bearer token

**Client Instance**:
An independently authorized browser tab through which one external user participates in Sessions. Its ID is held in `sessionStorage`, survives a reload of that tab, and disappears when the tab closes; a new tab receives a different ID and may coexist for the same user. The IndexedDB execution journal is partitioned by this ID. Credential rotation supersedes only an earlier credential for the same Client Instance.
_Avoid_: External Identity, Hub User, Session, globally exclusive login

**Anonymous Client**:
A browser-only consumer of a public Integration App with no trusted External Identity or application backend. Its browser-local visitor identity may recover one exact current Session, but it cannot discover conversation history across Sessions.
_Avoid_: Hub User, External Identity, authenticated client, permanent anonymous account

**Client Tool**:
A structured capability defined by a trusted Integration App owner or backend as `{ name, description, input_schema }`, exposed to the Agent and executed by the application's client rather than by Hub or the Runtime. A name is unique within one Grant, contains only letters, digits, `_`, and `-`, and is at most 64 characters; one Grant contains at most 128 tools and at most 256 KB of serialized tool definitions, and every `input_schema` is an object-type JSON Schema. Client Tools are available only when the Agent permits its `integration` capability. Hub translates the protocol-neutral shape to an internally namespaced Execution Engine tool name so it cannot collide with built-in tools, while all Client API and SDK events retain the application's original name. Hub and its SDK do not impose a generic side-effect confirmation dialog: each application handler decides whether to use its own confirmation UI, and a refusal produces a terminal `user_rejected` Client Tool Result.
_Avoid_: Pi built-in Tool, MCP Tool, Skill, arbitrary client command

**Client Tool Grant**:
The set of Client Tools authorized for one Client Instance. A trusted Integration App backend may submit complete tool definitions (`name`, `description`, and JSON Schema) when obtaining a Client Access Credential; Hub validates and freezes those definitions into the Grant. An Anonymous Client cannot submit tool definitions and receives only the Integration App's preconfigured Client Tool definitions stored as JSON on the existing Integration App record and managed through list/add/edit/delete forms. The Grant authorizes future Runs and is not a permanent property of Session history.
_Avoid_: Agent tool policy, tools invented by the browser, Session Tool Set

**Run Tool Snapshot**:
The immutable Client Tool set captured from the initiating Client Instance's current Grant for one Run. Tool-result continuation Runs retain that snapshot even if the Client Instance is reauthorized later.
_Avoid_: Live Client Tool Grant, Session Tool Set, mutable tool catalog

**Run Tool Executor**:
The Client Instance whose credential authorized a Run and which alone may submit results for that Run's Client Tool requests. Other Client Instances may observe the Run without executing its tools.
_Avoid_: Session owner, Agent, every connected browser

**Client Tool Invocation**:
One immutable request to execute a Client Tool, identified by a stable `tool_call_id` and bound to its Run Tool Executor. It has at-most-once execution semantics: before calling an application handler, the SDK durably records the invocation and claims it from Hub; Hub confirmation is required before execution. Reconnect delivery to the same Client Instance may resend a cached result, but an invocation already recorded as executing with an unknown outcome is never run again automatically. A different Client Instance cannot claim, replay, or take over the invocation; external side effects must additionally use `tool_call_id` as an idempotency key at the application boundary. It has a five-minute hard deadline. An interruption or timeout fails the current Turn without model continuation or automatic retry, and SDK cancellation never implies that arbitrary application code was forcibly stopped. Repeating an identical result submission is idempotent, while a different result for the same `tool_call_id` conflicts. Invocation parameters, status, and result remain with Session history after credential expiry.
_Avoid_: exactly-once claim, automatic retry, cross-device replay, argument-based deduplication

**Client Tool Batch**:
The ordered Client Tool Invocations emitted by one model response. The SDK executes them serially in model-output order, and Hub waits until every invocation has a terminal success or failure result before starting one model continuation for the batch.
_Avoid_: parallel browser side effects, one continuation per tool result

**Client Tool Result**:
The persisted JSON outcome of one Client Tool Invocation. Success is `{ "status": "success", "output": <JsonValue> }`; failure is `{ "status": "error", "error": { "code": <string>, "message": <string>, "retryable": <boolean> } }`. The SDK converts thrown exceptions to the failure shape without transmitting JavaScript stacks, and neither Hub nor the SDK automatically retries an error result. Its UTF-8 JSON serialization is limited to 16,000 bytes; oversized results are rejected explicitly and never silently truncated.
_Avoid_: unstructured exception text, browser stack trace, implicit retry

**Browser SDK**:
The first-version framework-neutral TypeScript client for browser applications, published as the ESM package `@agent-hub/client` from `sdk/typescript/` with TypeScript declarations and no React, Vue, Node-only, or other framework runtime dependency. One `AgentHubClient` lists Sessions and opens either an existing Session or a local draft that is created on Hub only by its first message; a Session object pages messages, subscribes to typed live events, and sends messages. Across different Sessions one credential may operate concurrently, while each Session has only one active Turn; `send()` during that Turn immediately steers it and retains its original Run Tool Executor and Snapshot. Session SSE reconnects from the last event sequence, and message retries reuse a stable `client_message_key`. Tool registration maps already-authorized names to handlers and cannot redefine their descriptions or schemas; a missing handler produces terminal `tool_handler_not_registered` for that invocation without preventing later invocations in the same batch. Authenticated and anonymous connection helpers share the same Session API. For authenticated use, the SDK creates its tab-scoped Client Instance ID first and passes it to an application-provided `authorize` callback; the application backend combines that ID with trusted user identity and complete tool definitions when requesting the credential from Hub, and its `client_secret` never reaches the browser. The SDK normally renews its 15-minute credential directly with Hub without changing the Grant; after a renewal `401` it invokes `authorize` once, and explicit `reauthorize()` obtains a replacement Grant without changing an in-flight Run Snapshot. The SDK owns Client Tool dispatch. Its at-most-once execution journal uses IndexedDB by default, is partitioned by Client Instance ID, and accepts a custom storage adapter for tests or application-managed storage; acknowledged terminal entries are removed after 24 hours, while unknown entries remain until Hub reports a terminal state. Client Access Credentials remain in page memory and are never persisted by the SDK: authenticated pages invoke `authorize` after reload, while anonymous pages use their persisted visitor key to obtain a fresh credential and recover only the current Session. Integration App backends use language-neutral HTTP APIs documented with complete request, response, security, vanilla TypeScript, and React examples; V1 does not ship separate React hooks or a CDN artifact. The package must pass `npm pack` verification but is not published by this task, and the current Widget consumes the same core SDK.
_Avoid_: Node-only SDK, framework runtime dependency, client secret in browser, undocumented server exchange

**Browser Origin Policy**:
An optional per-Integration-App exact-Origin allowlist for authenticated Browser SDK and Widget requests. An authenticated Integration App may omit it to accept any Origin; an Anonymous Client application must configure at least one Origin and Hub permits only exact matches. Both HTTP and HTTPS origins are accepted in production even though documentation must explain bearer-token interception and mixed-content risks. Server-to-server credential issuance is not governed by browser CORS.
_Avoid_: mandatory HTTPS enforcement, mandatory Origin configuration

**Client API**:
The canonical `/api/client/*` HTTP and SSE surface used by both the Browser SDK and Widget for credential renewal, Session history and messages, Run stop, Client Tool claim, and Client Tool Result submission. Existing `/api/widget/*` paths remain compatibility aliases but are not the primary documented contract. Session streams resume by event sequence; message delivery retries use `client_message_key`; identical Client Tool Result retries are idempotent and divergent retries conflict. Typed tool request, result, timeout, and error events remain separate from assistant text; the built-in Hub and Widget conversation views render them in message order as collapsible technical events with tool name, status, and elapsed time, while external clients control their own presentation.
_Avoid_: Widget-only public contract, automatic message duplication, divergent result overwrite

**External User Context**:
The trusted external profile snapshot attached to one Widget Run by Hub after Integration App authentication. It carries the External Tenant and external user ID plus optional username, display name, email, and attributes to the Execution Engine; browser input alone cannot change it.
_Avoid_: Hub User profile, untrusted prompt text, live mutable identity record

**Widget History**:
The optional per-Integration-App discovery of Sessions constrained by Integration App, Agent, External Platform, External Tenant, External Identity, and external user ID. Disabling it removes list discovery but does not invalidate an exact Session ID already held by the current Widget page.
_Avoid_: Cross-application history, Hub console Session list, permanent browser archive

**Agent Scope**:
An OAuth permission named `agent:<uuid>` that authorizes an Application Token to use one currently delegated Agent. It never changes a Session's immutable Agent binding or Session Origin.
_Avoid_: Agent visibility, Application ownership, Session Origin

**Agent Execution Configuration**:
The current versioned instructions, associated Skills, model and reasoning choices, sandbox and approval policies, and MCP or tool access that an Agent supplies to a new Turn. It excludes user credentials and Session-owned state.
_Avoid_: Agent profile, user credentials, active Turn snapshot

**Secret Variable**:
A named secret owned by one Hub User, containing either a value or a file, stored encrypted by Hub and never returned in plaintext after creation. It is the user-side counterpart of an Agent Secret Declaration.
_Avoid_: API Key, model provider secret, Agent secret, CI/CD variable

**Agent Secret Declaration**:
A named variable key declared by one Agent, with a kind (environment variable or file) and a usage hint, that references no concrete user secret. The Agent sees only the key, never whether any user owns or grants the secret.
_Avoid_: Agent-bound secret, managed secret, secret policy

**Secret Grant**:
A persistent authorization record binding one Hub User, one Agent, and one Secret Variable key, created after the user allows use during a Session. It is remembered by default and can be revoked; revocation affects new Runs only.
_Avoid_: one-time approval, session-scoped consent, secret subscription

**Secret Injection**:
The Runtime-side materialization of authorized secrets into a Session: values become AGENT_SECRET_<KEY> environment variables, files become read-only files under the Session's private engine-state/secrets directory with AGENT_SECRET_FILE_<KEY>. It never writes secrets into the Workspace or Session Bundle.
_Avoid_: workspace secret file, shell-visible secret, bundle-backed secret

**Model API Connection**:
A reusable provider access configuration containing one endpoint, credential, API type, and set of allowed model identifiers. It is either Global or Personal and does not define an Agent's model invocation settings.
_Avoid_: Model Connection, Model, provider environment variables

**Allowed Model ID**:
An exact provider model identifier permitted through one Model API Connection. It is a restriction on that connection rather than a separately owned Model resource.
_Avoid_: Model Connection, model record

**Model Gateway**:
The stateless internal data-plane process that receives one Hub-authorized Responses request with its request-scoped endpoint, protocol, and credential, then transparently forwards OpenAI Responses or converts Anthropic Messages back to Responses JSON/SSE. It owns no Model API Connections, provider keys, authorization policy, usage ledger, retry, or persistent state.
_Avoid_: Model API Connection manager, business control plane, provider key store, Runtime proxy

**Global Model API Connection**:
A system-wide Model API Connection managed by an Administrator and available for model selection by every Agent in Agent Hub.
_Avoid_: Shared personal model, default model

**Personal Model API Connection**:
A Model API Connection owned by one Hub User and available for model selection only by Agents owned by that same Hub User.
_Avoid_: Private global model, per-Agent credential

**System Default Model Selection**:
The pair of one Global Model API Connection and one of its Allowed Model IDs copied into a newly created Agent. Changing the system default does not change existing Agents.
_Avoid_: System Default Model Connection, dynamic global override, fallback model

**Agent Model Selection**:
The pair of one permitted Model API Connection and one of its Allowed Model IDs selected for an Agent's primary execution. Subagent Definitions inherit that pair unless they explicitly select another permitted pair.
_Avoid_: Agent Default Model Connection, System default, fallback chain

**Agent Model Settings**:
The model invocation choices owned by an Agent and inherited by its Subagent Definitions unless they explicitly override them. They exclude provider endpoints and credentials.
_Avoid_: Model API Connection settings, provider credential

**Run Model Binding**:
An immutable, non-secret routing snapshot created for one Run and one effective main-Agent or Subagent model configuration. Runtime sends its UUID to Hub so two callers using the same Model API Connection and Allowed Model ID can still use different Agent Model Settings without exposing provider access details.
_Avoid_: Model API Connection ID, Model record, provider credential

**Model-Unconfigured Agent**:
An Agent whose required Agent Model Selection is absent or no longer permitted, including after its Model API Connection is force-deleted or its Allowed Model ID is removed. Its configuration and history remain viewable, but it cannot start a new Turn until a valid selection is made.
_Avoid_: Deleted Agent, Historical Session, automatic fallback

**Subagent Definition**:
A uniquely named child role within one Agent, with its own description, Markdown instructions, and optional Model Selection and Model Settings overrides. It shares the parent Agent's Workspace, Skills, MCP, and sandbox authority, does not create another Hub Session, and becomes model-unconfigured rather than silently inheriting when an explicit override is removed.
_Avoid_: Agent Hub Agent, separate Hub Session, independently owned Agent

**Model Token Usage**:
Token consumption reported by a model service and attributed both to the Agent whose work caused it and to its initiating subject: the calling Hub User, an Automation's owning Hub User, a user-level Application Token's Hub User, or an app-only Integration App. Once recorded, it remains in historical model totals after related Users, Agents, Sessions, Runs, or Model API Connections are deleted, with erased user attribution anonymized.
_Avoid_: Agent owner usage, estimated billing, Session ownership

**Model Call Error**:
A durable, sanitized record of a failed model request or an invalid Responses API result. It follows the same retention and anonymization lifecycle as Model Token Usage but contains no prompt, model output, request or response headers, or credential.
_Avoid_: Raw provider response, application log, Token Usage

**Conversation Draft**:
A Hub console state belonging to one Hub User and one selected Agent but with no accepted message. Each Agent may retain one local Conversation Draft for that Hub User until its first message is accepted, the Draft is discarded, or the Hub User signs out. It owns no Session, Run, Workspace, Runtime ownership, or Native Session.
_Avoid_: Empty Session, draft Session, initial Run

**Session Origin**:
The trust boundary through which a Session was created. It is either Hub-native, with no external identity, or external, scoped to one External Platform, External Tenant, and External Identity.
_Avoid_: Fake Hub platform, channel, unscoped source

**Session**:
A durable conversation owned by one Hub User, permanently assigned to one Agent, and created with one Session Origin after its first message is accepted. While executable, it continues through one Native Session; an external integration cannot access a Hub-native Session or a Session from another external origin without an explicit transfer or sharing grant.
_Avoid_: Run, Native Session, unscoped integration session

**Historical Session**:
An immutable, view-only Session retained after its assigned Agent is deleted. It preserves conversation and Run history but has no Workspace or Agent configuration from which execution can resume.
_Avoid_: Archived Session, resumable Session, deleted history

**Recovery-Failed Session**:
A Session whose latest Hub history cannot be matched to one complete, restorable Workspace and Native Session. Its available history and last immutable Bundle remain viewable, execution stays disabled, and Hub never silently rolls it back or replaces its Native Session.
_Avoid_: Historical Session, deleted Session, recovered Session

**Workspace**:
The isolated, mutable project filesystem owned by exactly one Session. Conversation Turns and their Hub execution records operate serially on the same Workspace.
_Avoid_: Run directory, disposable checkout, Execution Engine state

**Run**:
One Hub-owned scheduling and audit record for queued or active work, normally associated with one Native Turn. It is not an Execution Engine conversation primitive and does not own a separate logical Workspace.
_Avoid_: Session, Native Turn, Workspace snapshot

**Steering Message**:
A user message submitted while a Native Turn is active and intended to redirect that same Turn. Ordinary messages received during active work use this intent unless the sender explicitly asks to wait for the next Turn; every accepted Steering Message remains a separate history item even when several belong to one Turn, and all use the Agent and Skill configuration captured when that Turn began.
_Avoid_: Queued follow-up, new Turn, interrupt

**Interrupted Turn**:
A Native Turn stopped before normal completion at the user's explicit request. Its recorded output, completed actions, and resulting Workspace changes remain part of the Session; interruption does not imply rollback.
_Avoid_: Reverted Turn, deleted Turn, failed Turn

**Session History**:
The Hub-owned authoritative ordered record of model-visible conversation items for one Session. It durably mirrors the Native Session and is distinct from both the UI projection of messages and Hub Run events.
_Avoid_: Native Session file, Run events, chat transcript

**Native Session**:
The Execution Engine conversation paired with one executable Hub Session and resumed across its Native Turns. Its process may stop and restart, but Hub does not create a fresh Native Session for every Run; its identifier remains an execution mapping rather than the Hub Session's ownership or access-control identity.
_Avoid_: Hub Session, Run, disposable execution projection

**Native Turn**:
One Execution Engine interaction within a Native Session. A Hub Run normally schedules and audits one Native Turn, while Steering Messages may join the active Native Turn without creating another one.
_Avoid_: Run, Hub Session, model request

**Session Recovery Data**:
The smallest non-Workspace data set needed to restore one Session and resume its Native Session: Session and Native Session identifiers, bundle version and history checkpoint, plus the required native transcript files. It excludes credentials, secrets, logs, caches, Skills, and Hub-regenerable Agent configuration.
_Avoid_: Session metadata, full Execution Engine state, Session History

**Session Bundle**:
The single current immutable archive generation containing exactly one Session's Workspace and Session Recovery Data. The recovery data records the Runtime Engine Version that produced it, while restoration uses the engine built into the current Runtime image; no other runtime files or historical Bundle generations are retained.
_Avoid_: Workspace-only tarball, full Execution Engine state backup
