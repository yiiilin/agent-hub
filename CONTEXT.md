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
The Agent Hub identity that owns user-scoped sessions, credentials, and stored workspace data. Agent ownership and session-user ownership are distinct relationships.
_Avoid_: Agent owner when referring to the user participating in a session

**Super Administrator**:
The Hub User with unrestricted platform-wide authority, including authority over Administrators, other Super Administrators, and their protected accounts and personally owned resources. The first Hub User created in an empty Agent Hub becomes its initial Super Administrator.
_Avoid_: Agent owner, ordinary administrator, first external user

**Administrator**:
A Hub User that may manage system-wide resources and every non-Super-Administrator account and its data. It cannot inspect, modify, or delete a Super Administrator account, credentials, or personally owned resources; system-wide resources remain administrable regardless of who created them.
_Avoid_: Super Administrator, Agent owner, read-only auditor

**Authentication Channel**:
An administrator-approved source through which a person authenticates as a Hub User. An enabled external Authentication Channel is trusted to assert the email identity used to bind that channel to a Hub User.
_Avoid_: Untrusted email input, Integration App, Session Origin

**Integration App**:
An application registered under one External Platform and Authentication Channel that may be delegated access to multiple Agents. It is the common integration boundary for OAuth clients, external Session APIs, and embedded Widgets.
_Avoid_: Agent, External Platform, Authentication Channel

**Application Token**:
A short-lived credential issued to an Integration App. It represents either the application itself or one authenticated Hub User, and its authority is limited by its Agent Scopes and the application's current Agent delegations.
_Avoid_: API Key, browser Session, Runtime Credential

**Agent Scope**:
An OAuth permission named `agent:<uuid>` that authorizes an Application Token to use one currently delegated Agent. It never changes a Session's immutable Agent binding or Session Origin.
_Avoid_: Agent visibility, Application ownership, Session Origin

**Username**:
The globally unique, human-facing identifier assigned to one Hub User. Its initial value may reflect an External Identity, but later external profile changes do not alter it; only an explicit Hub User action can rename it.
_Avoid_: Display name, email, external username, user ID

**Agent Execution Configuration**:
The current versioned instructions, associated Skills, model and reasoning choices, sandbox and approval policies, and MCP or tool access that an Agent supplies to a new Turn. It excludes user credentials and Session-owned state.
_Avoid_: Agent profile, user credentials, active Turn snapshot

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
