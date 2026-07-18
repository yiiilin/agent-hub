# Console Workflows And Integration Apps Development Plan

Implement the approved management-console workflows and the Integration App, Skill deletion, API Key expiration, and Runtime configuration contracts recorded in ADR-0022 and ADR-0023. Plain Markdown remains the persisted format, Session Origin and Agent binding remain immutable, and generated Runtime configuration never becomes Workspace data.

## Execution Rule

Progress must be tracked in this file only:

- Start every pending task as unchecked
- Mark each task with `[x]` immediately after implementation and verification complete
- If a task reveals a prerequisite gap, add a new unchecked task directly below it before continuing
- If any task remains unchecked, the project is not complete

## Tasks

- [x] **Task 1: Update the domain and specification baseline.** Add the Integration App, Application Token, and Agent Scope vocabulary; record the Integration App and Skill deletion decisions; update the Integration, Auth, Skill, Agent, MCP, Automation, Session Runtime, Codex Driver, Operations, and V1 specs; mark the root greenfield plan historical. Verification: targeted stale-contract scan over current normative docs.
- [x] **Task 2: Change database and public contracts.** Add destructive development-only migrations for Integration Apps, Agent delegations, OAuth grants/scopes, API Key expiration, physical Skill deletion, online configuration refresh state, and external-platform/user administration. Synchronize shared DTOs, OpenAPI, API client types, permission checks, and focused database tests. Verification: shared tests plus focused backend contract and ignored SQLx tests.
- [x] **Task 2A: Make External Identity tenant-aware.** Add the External Tenant to identity persistence and uniqueness, keep ordinary authentication on the `default` tenant, and prove identical external user identifiers remain isolated across tenants. Verification: full migration-chain replay and a focused ignored SQLx identity test.
- [x] **Task 3: Add shared Markdown and form surfaces.** Add `@mdxeditor/editor@4.0.4`, one reusable WYSIWYG/source editor, responsive dialog/drawer primitives, Session-first navigation, and centralized i18n. Verification: TypeScript check and focused editor interaction tests.
- [x] **Task 4: Complete Agent, Skill, MCP, and Runtime refresh workflows.** Remove inline Skills, enforce public visibility roles, show enabled managed Skills through a selector, render MCP as a redacted table with subforms, physically and bulk delete Skills, and apply generation-fenced configuration refreshes at safe Turn boundaries. Verification: focused Rust/runtime tests and Agent/Skill/MCP browser tests.
- [x] **Task 5: Complete Integration Apps.** Build Integration App CRUD-without-delete, multi-Agent delegation, secret rotation, both OAuth grants, profile and Agent scopes, userinfo, immediate permission invalidation, external Session authorization, and per-Agent short-lived Widget links. Verification: focused backend/OAuth tests and Integration browser flows.
- [x] **Task 5A: Expose Integration App setup options and secure Widget issuance.** Let any authenticated App owner read only enabled trusted External Platform/Authentication Channel options, issue a one-hour Widget session for one currently delegated Agent through the owned Integration App, and prevent user-subject `authorization_code` tokens from shedding their permission boundary through the app-only Widget exchange. Synchronize shared DTOs, OpenAPI, API client methods, and focused owner/delegation/invalidation tests before building the console flow.
- [x] **Task 6: Rebuild the Session workspace.** Make Sessions the default route, provide a conversation-first list/chat layout, source filter, Agent chooser, SSE updates, folded technical events, stop/steer actions, and Historical Session read-only behavior. Verification: focused desktop and 390px browser flows.
- [x] **Task 7: Complete remaining management workflows.** Make Automations list-first with Markdown forms; add API Key validity, same-token renewal and delete-only controls; restructure Runtime enrollment; and split Administration into authentication, external-platform, user-management, and Codex-version tabs. Verification: focused backend tests and browser workflows.
- [x] **Task 8: Deliver verified work.** Run the required frontend build, focused Rust and Playwright acceptance, one independent read-only blocking review, then at most one final full gate. Record non-blocking recommendations without reopening implementation. Verification: no unresolved blocking finding and every required command passes.
- [x] **Task 8A: Reconcile interrupted restoring Runs after Runtime restart.** Include the active Run in Runtime-owned Session snapshots so a Runtime that recovers a `restoring` Session without valid local supervisor metadata can fail that Run through the existing generation-fenced completion contract, release blocked capacity, and preserve Recovery-Failed Session semantics even when the successful failure response is lost. Verification: focused shared, backend, Runtime recovery, and lost-ack fencing tests plus the previously blocked Widget acceptance flows.

## Completion Condition

The project is complete only when every task above is checked, the affected desktop and 390px workflows have fresh browser evidence, and the independent review has no valid unresolved blocker.
