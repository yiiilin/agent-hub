# Agent Hub

Agent Hub coordinates isolated agent sessions across external platforms and runtime nodes.

## Language

**Active Codex Version**:
The single Codex CLI release selected by Agent Hub for newly started Runs across every runtime architecture and external platform. Architecture-specific artifacts must share the same release tag; an in-flight Run may finish on the version with which it started.
_Avoid_: Runtime Codex version, platform Codex version

**Target Codex Version**:
The concrete Codex CLI release explicitly selected by an administrator for a future global activation. Agent Hub does not activate a mutable `latest` alias directly.
_Avoid_: Latest Codex, floating Codex version

**Runtime**:
A registered worker node that owns isolated local Session execution directories and runs Codex. Ordinary deletion first drains its Sessions safely; force deletion declares the node permanently lost and prevents it from participating again under its former identity.
_Avoid_: Agent, Session, app-server process

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

**Model Connection**:
A reusable connection to one Responses API-compatible model endpoint, including the model identity and the credential needed to invoke it. A Model Connection is either Global or Personal and may be assigned only to Agents within that scope.
_Avoid_: Model policy, provider environment variables, Agent credential

**Global Model Connection**:
A system-wide Model Connection managed by an Administrator and available for assignment to every Agent in Agent Hub.
_Avoid_: Shared personal model, default model

**Personal Model Connection**:
A Model Connection owned by one Hub User and available for assignment only to Agents owned by that same Hub User.
_Avoid_: Private global model, per-Agent credential

**System Default Model Connection**:
The one Global Model Connection copied into a newly created Agent as its Agent Default Model Connection. Changing the system default does not change existing Agents.
_Avoid_: Dynamic global override, fallback model

**Agent Default Model Connection**:
The Model Connection selected for an Agent's primary Codex work and inherited by its Codex Subagent Definitions unless they explicitly select another permitted connection.
_Avoid_: System default, fallback chain, provider environment variable

**Model-Unconfigured Agent**:
An Agent whose required Agent Default Model Connection is absent, including after that connection is force-deleted. Its configuration and history remain viewable, but it cannot start a new Turn until an available Model Connection is selected.
_Avoid_: Deleted Agent, Historical Session, automatic fallback

**Codex Subagent Definition**:
A uniquely named child role within one Agent, with its own description, Markdown instructions, and optional model and reasoning overrides. It shares the parent Agent's Workspace, Skills, MCP, and sandbox authority, does not create another Hub Session, and becomes model-unconfigured rather than silently inheriting when an explicit override is force-deleted.
_Avoid_: Agent Hub Agent, separate Hub Session, independently owned Agent

**Model Token Usage**:
Token consumption reported by a model service and attributed both to the Agent whose work caused it and to its initiating subject: the calling Hub User, an Automation's owning Hub User, a user-level Application Token's Hub User, or an app-only Integration App. Once recorded, it remains in historical model totals after related Users, Agents, Sessions, Runs, or Model Connections are deleted, with erased user attribution anonymized.
_Avoid_: Agent owner usage, estimated billing, Session ownership

**Model Call Error**:
A durable, sanitized record of a failed model request or an invalid Responses API result. It follows the same retention and anonymization lifecycle as Model Token Usage but contains no prompt, model output, request or response headers, or credential.
_Avoid_: Raw provider response, application log, Token Usage

**Session Origin**:
The trust boundary through which a Session was created. It is either Hub-native, with no external identity, or external, scoped to one External Platform, External Tenant, and External Identity.
_Avoid_: Fake Hub platform, channel, unscoped source

**Session**:
A conversation owned by one Hub User, permanently assigned to one Agent, and created with one Session Origin. While executable, it continues through one native Codex Thread; an external integration cannot access a Hub-native Session or a Session from another external origin without an explicit transfer or sharing grant.
_Avoid_: Run, Codex Thread, unscoped integration session

**Historical Session**:
An immutable, view-only Session retained after its assigned Agent is deleted. It preserves conversation and Run history but has no Workspace or Agent configuration from which execution can resume.
_Avoid_: Archived Session, resumable Session, deleted history

**Recovery-Failed Session**:
A Session whose latest Hub history cannot be matched to one complete, restorable Workspace and native Codex Thread. Its available history and last immutable Bundle remain viewable, execution stays disabled, and Hub never silently rolls it back or replaces its Thread.
_Avoid_: Historical Session, deleted Session, recovered Session

**Workspace**:
The isolated, mutable project filesystem owned by exactly one Session. Conversation Turns and their Hub execution records operate serially on the same Workspace.
_Avoid_: Run directory, disposable checkout, Codex home

**Run**:
One Hub-owned scheduling and audit record for queued or active work, normally associated with one Codex Turn. It is not a native Codex conversation primitive and does not own a separate logical Workspace.
_Avoid_: Session, Codex Turn, Workspace snapshot

**Steering Message**:
A user message submitted while a Codex Turn is active and intended to redirect that same Turn. Ordinary messages received during active work use this intent unless the sender explicitly asks to wait for the next Turn; every accepted Steering Message remains a separate history item even when several belong to one Turn, and all use the Agent and Skill configuration captured when that Turn began.
_Avoid_: Queued follow-up, new Turn, interrupt

**Interrupted Turn**:
A Codex Turn stopped before normal completion at the user's explicit request. Its recorded output, completed actions, and resulting Workspace changes remain part of the Session; interruption does not imply rollback.
_Avoid_: Reverted Turn, deleted Turn, failed Turn

**Session History**:
The Hub-owned authoritative ordered record of model-visible conversation items for one Session. It durably mirrors the native Codex Thread and is distinct from both the UI projection of messages and Hub Run events.
_Avoid_: Codex rollout, Run events, chat transcript

**Codex Thread**:
A native Codex conversation paired with one executable Session and resumed across its Turns. Its process may stop and restart, but Hub does not create a fresh Thread for every Run; its identifier remains an execution mapping rather than the Hub Session's ownership or access-control identity.
_Avoid_: Session, Run, disposable execution projection

**Session Recovery Data**:
The smallest non-Workspace data set needed to restore one Session and resume its native Codex Thread: Session and Thread identifiers, bundle version and history checkpoint, plus the required native transcript and index files. It excludes credentials, secrets, logs, caches, Skills, and Hub-regenerable Agent configuration.
_Avoid_: Session metadata, full Codex home, Session History

**Session Bundle**:
The single current immutable archive generation containing exactly one Session's Workspace and Session Recovery Data. The recovery data records the Codex version that produced it, while restoration uses the current Active Codex Version; no other runtime files or historical Bundle generations are retained.
_Avoid_: Workspace-only tarball, full Codex home backup
