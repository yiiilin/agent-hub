# Native Codex Session Runtime Development Plan

## Goal

Implement the approved Agent Hub identity, Session, Runtime, Codex app-server, Bundle, and version-management architecture recorded in ADR-0001 through ADR-0021. The finished system keeps one native Codex Thread per executable Hub Session, isolates every Session workspace, supports steering and interruption, and can move an offline Session between Runtime nodes without exposing object-storage credentials.

This plan replaces the obsolete per-Run execution assumptions in the root `plan.md`; it does not restart or repeat unrelated V1 feature work.

## Source Of Truth

The behavioral contracts are ADR-0001 through ADR-0021 plus `docs/auth-spec.md`, `docs/agent-management-spec.md`, `docs/integration-spec.md`, `docs/skills-spec.md`, and the official Codex app-server protocol. Where an older feature spec still says that a Run owns a new workspace, `CODEX_HOME`, or app-server process, the later ADRs win.

The app-server integration must preserve Codex's native lifecycle:

```text
Hub Session 1 --- 1 Codex Thread
      |
      +--- many ordered Hub Messages
      +--- many Hub Runs (scheduling and audit)
                         |
                         +--- one Codex Turn at a time
                                  +--- turn/steer while active
                                  +--- turn/interrupt on explicit stop
```

Identity and origin ownership must follow this relationship:

```text
External Platform 1 --- many External Identities
External Identity many --- 1 Hub User
Hub User 1 --- many Sessions
Agent 1 --- many Sessions (immutable binding)

Hub-native Session: origin platform/tenant/identity are empty
External Session:   origin platform + tenant + identity are all fixed
```

## Non-Goals

This work does not add cross-platform Session sharing, Bundle history or rollback, automatic Thread replacement after recovery failure, interruption rollback, Runtime-to-S3 access, mutable `latest` activation, or unrelated UI improvements. Existing product features are migrated only as needed to use the universal Session lifecycle.

## Execution Rule

Progress must be tracked in this file only:

- Start every pending task as unchecked
- Mark each task with `[x]` immediately after implementation and verification complete
- If a task reveals a prerequisite gap, add a new unchecked task directly below it before continuing
- If any task remains unchecked, the project is not complete

Execute exactly one task at a time. Tests for a task must first demonstrate the missing behavior where practical, then pass after the implementation. Do not batch checkbox updates. Use one implementation Agent for all tasks and one separate read-only reviewer at the end; neither may delegate further.

## Implementation Tasks

- [x] **Task 1: Consolidate the approved contracts and freeze protocol fixtures.** Update `docs/codex-driver-spec.md`, `docs/v1-spec.md`, `docs/agent-management-spec.md`, `docs/skills-spec.md`, `docs/integration-spec.md`, and add a focused Session Runtime spec so they no longer prescribe per-Run workspaces or disposable app-server processes. Record the exact Session state machine, message ordering, ownership generation, Bundle contents, Runtime-only-to-Hub boundary, and exact-version rollout rules. Extend the fake app-server fixture and focused Runtime protocol tests to recognize `thread/resume`, `turn/start`, `turn/steer`, and `turn/interrupt` with real request/response identifiers, while retaining existing V1 test behavior. Verification: `cargo test -p agent-hub-runtime app_server` and a stale-contract `rg` check over the updated specs.

- [x] **Task 2: Add the Hub identity and authentication-policy foundation.** Add migrations, shared DTOs, backend APIs, and focused database tests for globally unique Username, optional password credential, separately configurable password registration/login and email verification, first-user atomic Super Administrator bootstrap, administrator-enabled trusted external platforms/channels, stable external identities, trusted-email auto-binding, and deterministic Username collision suffixes. Preserve existing password users without a data migration beyond schema backfill. Verification: focused shared/backend unit tests plus ignored SQLx identity tests against the local test PostgreSQL database.

- [x] **Task 3: Introduce the universal Hub Session and ordered message model.** Add migrations and shared DTOs for one Session identity across console, widget, and integration origins; immutable owner and Agent binding; fixed external platform/tenant/identity origin; ordered distinct messages; Codex Thread and active Turn identifiers; Hub Run linkage; lifecycle/recovery status; configuration fingerprint; ownership generation; and current Bundle metadata. Backfill existing embed/integration conversations without pretending Hub-native Sessions have external origins. Verification: shared serialization tests and focused ignored SQLx migration/invariant tests, including uniqueness, origin scoping, message ordering, and immutable Agent binding.

- [x] **Task 4: Route all conversation entry points through universal Sessions.** Replace source-specific continuation logic in backend APIs and frontend client types so console, widget, and integration messages create or reuse the same Session abstraction while enforcing their distinct access boundaries. Serialize executable work per Session, preserve every accepted message separately, combine restore-time ordinary messages into the same upcoming Turn in accepted order, and leave explicitly deferred messages queued. Keep Hub Runs as scheduling/audit records. Verification: focused backend tests for native/external authorization, idempotency, concurrent message ordering, one active Run per Session, and console/widget/integration adapters.

- [x] **Task 5: Replace the shared Runtime registration token with enrollment and per-Runtime credentials.** Add one-time 30-minute enrollment tokens, hash-only Hub storage, single atomic consumption, immutable Runtime identity, OS-protected Runtime credential persistence, authenticated restart without hostname re-registration, revocation, and Runtime-completed non-disruptive credential rotation. Update shared DTOs, backend/runtime configuration, Compose bootstrap, admin APIs, and tests without exposing plaintext after the one allowed response. Verification: focused backend/runtime tests for expiry, replay races, revoked/deleted credentials, offline rotation, restart persistence, log/payload redaction, and successful old-to-new credential handoff.

- [x] **Task 6: Enforce exclusive Session ownership and Runtime drain/delete semantics.** Implement transactional ownership acquisition with a monotonically increasing generation and require that generation on every Session command, event, heartbeat-owned state update, and Bundle commit. Reject stale owners. Add Runtime draining, cancel-drain, ordinary delete after all Sessions release, and explicit force delete that revokes credentials, invalidates generations, and marks only uncheckpointed Sessions recovery-failed. Verification: focused ignored SQLx race tests for competing claims, stale writes, drain with active/idle Sessions, last-Runtime deletion, blocked Bundle upload, cancel drain, and force deletion.

- [x] **Task 7: Build a persistent per-Session Runtime supervisor and filesystem layout.** Replace per-Run work directories with one persistent Session directory containing isolated `workspace/`, a separate generated Codex directory, local supervisor metadata, and transient Bundle staging. Recover owned online Sessions after a Runtime process restart. Keep one app-server child and one initialized transport connection per online Session, with deterministic cleanup and bounded concurrency. Verification: focused Runtime tests for isolation, restart discovery, child lifecycle, resource cleanup, and two concurrent Sessions with no path/config leakage.

- [x] **Task 8: Drive one native Codex Thread through multiple Turns, steering, and interruption.** Start a Thread once, resume its native Thread after restoration, use `turn/start` for each idle user round, bind active-Turn messages to `turn/steer`, fall back to the next Turn only when Codex reports the expected Turn already ended, and use `turn/interrupt` for explicit stop without rollback. Persist distinct Hub history items and map native Items/events to the correct Hub Run and Turn. Verification: protocol-level Runtime tests plus backend/runtime integration tests for multi-Turn continuity, steering races, interruption with retained effects/events, and no double steering into a newer Turn.

- [x] **Task 9: Synchronize Agent and Skill files only between Turns.** Define a deterministic complete execution-configuration fingerprint including Agent policy and every managed/inline Skill revision and checksum. Materialize Hub-managed Agent/Skill/MCP/config files into the Session Codex directory only before `turn/start` when the fingerprint changes; never mutate an active Turn, restart app-server solely for content changes, or infer reload behavior. Verification: focused backend/runtime tests for unchanged fingerprints, shared-Skill-only edits, updates during an active Turn, inline Skill precedence, and cross-Session isolation.

- [x] **Task 10: Implement the 15-minute online/idle lifecycle and recovery states.** Add configurable idle timing with a production default of 15 minutes; cancel the timer for a new Turn; checkpoint immediately for version switches and Runtime drain; never stop an active Turn for idleness. Model saving, waiting-for-runtime, restoring, online, offline, and recovery-failed states. Preserve queued messages during saving, retry failed saves while retaining local state, and reject stale-Bundle restoration when Hub history is newer. Verification: Tokio paused-time tests and focused control-plane tests for every timer/race/failure branch without wall-clock sleeps.

- [x] **Task 11: Implement safe `tar.zst` Session Bundles and Hub-streamed object storage.** On Runtime, create and restore archives with exactly `workspace/`, `manifest.json`, and minimal `codex-thread/`; include hidden files and `.git`; preserve safe symlinks; reject special files, traversal, and escaping links; compute/verify compressed checksum and declared sizes. On Hub, add a streaming object-store boundary with S3-compatible HTTP/HTTPS configuration, optional server-side encryption, a configurable 10 GiB default limit, atomic current-generation commit, and old-object cleanup only after commit. Hub must neither buffer the full body, unpack, scan, nor checksum it, and Runtime must never receive S3 credentials or URLs. Verification: archive security/property tests, streaming/backpressure and interrupted-transfer tests, generation atomicity database tests, and a local S3-compatible integration test.

- [x] **Task 12: Replace Agent archive with deletion plus Historical Sessions.** Implement irreversible Agent deletion that cancels unfinished Runs, removes executable configuration, credentials, automations, local workspace commands, and Bundle objects, but keeps the approved display snapshot, messages, completed Run/tool records, and historical attachments as read-only Sessions. Ensure neither Hub nor external APIs can continue them, while later Hub User erasure still removes them. Verification: focused database concurrency and authorization tests covering active work cancellation, secret removal, history visibility, and rejection of new messages.

- [x] **Task 13: Implement irreversible Hub User erasure.** Add Super Administrator APIs and a retryable deletion job that immediately blocks login and active execution, then purges the user's credentials, Agents, active Session data, attachments, local-workspace commands, and Bundle objects while retaining only the minimal audit tombstone required by ADR-0009. Require exact Username confirmation and keep email/Username unavailable until purge completion. Verification: focused authorization, idempotency, failure-retry, ownership-cascade, and audit-minimization database tests.

- [x] **Task 14: Distribute and roll one exact Codex CLI version across platforms.** Add administrator target-version APIs and a global candidate/active state. Hub downloads official GitHub release artifacts for every registered architecture, verifies published integrity, and serves bytes only to authenticated Runtimes. Each Runtime verifies the artifact, runs the bounded basic compatibility check, stores it by exact version, and reports readiness; promote only a concrete version that satisfies the platform policy. Active Turns finish on their old process, then old-version Session state is bundled; the next Turn starts with the new active binary. Do not run cross-version Session restoration tests, silently fall back, activate mutable `latest`, or let Runtime download from GitHub. Verification: mocked-release tests for architecture mapping/integrity/failure, Runtime compatibility-check tests, and rollout tests proving no Turn mixes versions.

- [x] **Task 15: Add Session, Runtime, identity, deletion, and version administration UI.** Extend the React console and centralized i18n with Session history/status, queued/saving/recovery-failed states, explicit stop, Runtime enrollment token and rotation controls, drain/delete/force-delete confirmations and affected Sessions, authentication policy/external-platform controls, user erasure, Agent deletion/history, and exact Codex version rollout status. Keep operational layouts compact and preserve existing responsive conventions. Verification: `npm run build` plus focused Playwright coverage at desktop and 390px for only these workflows, including console/network errors, no horizontal overflow, and real state transitions.

- [x] **Task 16: Run focused end-to-end acceptance and synchronize operational documentation.** Update Compose/configuration and operating docs for credential files, persistent Runtime storage, object storage, Bundle limits, exact-version rollout, drain/force-delete, and recovery-failed handling. Run the focused Rust integration tests, frontend build, and the new browser scenarios once against the same Compose project. Do not repeat unaffected fresh suites. Verification: all targeted commands listed by the preceding tasks pass in one recorded acceptance run.

  Acceptance record (2026-07-16): `agent-hub-dev` release images built and became healthy on `FRONTEND_PORT=15183`; focused PostgreSQL identity/Session/ownership/Runtime/Bundle/deletion/Codex rollout integration tests passed (9/9); focused Runtime supervisor/protocol/checkpoint/version tests and Bundle security tests passed (15/15); shared DTO tests passed (7/7); `npm run build` passed; and the new Session, Administration, Runtime administration, and Agent deletion Playwright scenarios passed (7/7) against that Compose project.

- [x] **Task 17: Complete one independent read-only blocking review.** A reviewer who did not implement the changes must compare the approved ADRs, this plan, the final diff/worktree, schema contracts, security boundaries, concurrency behavior, and fresh test evidence. Only valid blocking findings may reopen the implementation Agent's work; non-blocking recommendations are recorded in the delivery report and do not trigger refactoring. Verification: reviewer reports no unresolved valid blocking finding.

  - [x] Preserve immutable native Codex Thread identity when Agent deletion or Hub User erasure converts an executed Session to read-only history, with focused database regressions for both deletion paths.
  - [x] Bind each Integration access token atomically to its first Session origin tenant and External Identity, then enforce that complete origin on Session creation and access, including a cross-origin denial regression.

- [x] **Task 18: Run the single final full gate and close the plan.** Run at most once: `cargo fmt --all --check`, `cargo test --workspace`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cd frontend && npm run build`, and the required full Playwright suite against the already-built Compose environment. Do not rerun unaffected checks that already have fresh final-gate evidence. Fix only blocking regressions through the same implementation Agent, run the smallest failed check after a fix, and do not run a second full gate. Mark this item only when every earlier item is checked and no required check or review blocker remains.

  Execution override (2026-07-17): after the first full Playwright attempt exposed nine blocking regressions, the user explicitly authorized unrestricted repairs, parallel implementation Agents, and completion of every remaining task. The failed attempt is diagnostic evidence; after all directly failed specs pass, run one fresh final acceptance gate over the repaired worktree.

  - [x] Align final-gate E2E fixtures with immutable Session and complete-origin contracts: give fake Codex one native Thread per Session, issue a separate token for a second external origin, and delete fixture Session graphs before their Agent; rerun only the directly failed Playwright specs.
  - [x] Repair the three frontend-only mobile, accessibility, and localization fixture failures, with direct Playwright regressions at desktop and 390px where affected.
  - [x] Repair the universal Session, Run lineage, and irreversible Agent deletion API/fixture failures, preserving the approved history and origin contracts.
  - [x] Repair Runtime Session configuration inspection so MCP secrets and model-proxy configuration are verified against the persistent per-Session layout without lifecycle races.
  - [x] Make the MCP E2E teardown await Agent deletion and Runtime Session cleanup so the next scenario receives the released online-Session slot.
  - [x] Keep the app-server browser fixture within the configured four online-Session slots by releasing the console/widget Agent before the Automation phase and stopping periodic fixtures after their first observed run.
  - [x] Enforce Agent-before-Run/Session lock ordering in Runtime claim and update the archive/claim race regression to prove deletion wins without deadlock.
  - [x] Rerun all nine directly failed Playwright scenarios and confirm every original blocker is resolved before the final gate.
  - [x] Complete the fresh final acceptance gate and one independent read-only blocking review over the repaired worktree.

  Final acceptance record (2026-07-17): `cargo fmt --all --check`, `npm run build`, `cargo test --workspace` (backend 51 passed, Runtime 152 passed, shared 7 passed), strict workspace Clippy, and the nine repaired Playwright scenarios (9/9) passed. The single full Playwright run passed 85/86; its only failure showed that the app-server fixture's four Automation Sessions combined with one earlier legitimate online Session exceeded the configured four-slot Runtime capacity. The fixture was split into manual/webhook and interval/cron Agent phases without changing capacity or idle semantics; the only failed scenario then passed (1/1), and `npx tsc --noEmit` passed. The same independent read-only reviewer rechecked that final incremental repair and reported no blocking finding.

## Completion Criteria

The project is complete only when all 18 tasks are checked, every Session behavior in ADR-0001 through ADR-0021 is represented by fresh tests or direct browser evidence, the independent review has no unresolved valid blocker, and the single final gate has passed. Any remaining unchecked item means delivery is incomplete.
