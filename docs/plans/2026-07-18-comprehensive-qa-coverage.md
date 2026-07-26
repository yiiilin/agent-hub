# Comprehensive QA Coverage Delivery Plan

This plan implements `docs/qa-spec.md`. It expands the isolated unattended QA harness from three scenarios into traceable coverage for every Agent Hub V1 feature while preserving the existing Rust and Playwright suites.

## Execution Rule

Progress must be tracked in this file only:

- Start every pending task as unchecked
- Mark each task with `[x]` immediately after implementation and verification complete
- If a task reveals a prerequisite gap, add a new unchecked task directly below it before continuing
- If any task remains unchecked, the project is not complete

## Tasks

- [x] **Task 1: Establish the normative feature catalog.** Add `qa/features.json` with stable feature IDs derived from current specs, OpenAPI operations, UI routes, Widget behavior, and Runtime protocols. Record exact existing Rust/Playwright evidence without treating mock-only evidence as full-stack coverage. Synchronize `docs/v1-spec.md` and `qa/README.md` with the coverage contract. Verification: catalog schema validation, every referenced file/test marker exists, and every documented V1 domain is represented.

- [x] **Task 1a: Complete existing non-QA evidence registration.** Register the exact existing Rust tests for every feature whose required Rust layer was initially represented only by Playwright evidence. Verification: every required `rust` or `playwright` layer has a valid file/marker before new QA scenarios are counted.

- [x] **Task 2: Enforce coverage in the QA runner.** Extend scenario manifests with covered feature IDs and add `./qa/run-all.sh --coverage`. Reject unknown IDs, missing required layers, missing evidence, uncovered domains, unmapped OpenAPI operations, and unmapped UI routes. Include selected and overall coverage in JSON/JUnit summaries without starting Compose for the offline coverage command. Verification: focused Node tests or fixtures prove every invalid catalog/manifest shape fails, while the repository command accurately reports the still-pending scenario gaps; Task 12 is the first point at which repository coverage must reach 100%.

- [x] **Task 3: Cover Auth, API Key, and Administration identities.** Add deterministic API and browser scenarios for password/Mock OIDC sessions, policy gates, API Key validity/renewal/deletion, protected Super Administrator boundaries, role changes, password reset/session invalidation, user erasure, External Platform, and Authentication Channel workflows. All global policy changes must restore in `finally`. Verification: targeted API/browser scenarios and secret scans.

- [x] **Task 4: Cover Agent, Skill, and MCP workflows.** Add API and browser scenarios for Agent CRUD/visibility/model configuration, Markdown instructions, Subagent Definitions, Skill CRUD/bulk delete/Agent binding, MCP table CRUD/redaction/placeholder round-trip, owner boundaries, configuration refresh, Agent deletion, and Historical Session read-only behavior. Verification: targeted scenarios, Runtime materialization assertions, desktop/390px, and browser diagnostics.

- [x] **Task 5: Cover Model Connections, proxying, usage, and errors.** Add API and browser scenarios for Global/Personal scope, System Default copy semantics, connection CRUD/test/status/delete/force-delete, model-unconfigured rejection, Agent/subagent assignment, successful and failed Responses usage, immutable error history, attribution, time ranges, pagination, and deletion-safe snapshots. Verification: targeted scenarios against the fake Responses provider plus ledger/secret assertions.

- [x] **Task 6: Cover Session, Run, and native Codex behavior.** Extend fake fixtures only where needed for deterministic multi-Turn, hold/steer/interrupt, tool and failure states. Add API/browser scenarios for Session creation/source filtering, ordered messages, SSE events, technical event folding, same Thread continuation, independent Session isolation, pending/running/terminal states, steering, explicit stop without rollback, and read-only terminal lifecycles. Verification: targeted scenarios, desktop/390px, no stale browser errors, and native IDs/ordering assertions.

- [x] **Task 7: Cover Runtime, ownership, Bundle, recovery, and Codex rollout.** Add API/browser scenarios for enrollment token lifecycle, register/heartbeat, credential rotation, Runtime constraints, ownership generation rejection, drain/cancel/delete/force-delete, Bundle upload/download/checkpoint boundaries, recovery failure visibility, exact Codex target/readiness/promotion, and in-flight Run continuity. Destructive global rollout checks run last. Verification: targeted scenarios, object/runtime filesystem assertions, and restoration/cleanup checks.

- [x] **Task 7a: Close the Runtime deletion confirmation contract exposed by Task 7.** Add an administrator-only, read-only deletion-impact API that lists every affected Session and its force-delete disposition, then replace browser `window.confirm` flows with an explicit hostname-entry dialog that shows the current impact before drain/delete/force-delete. Keep the final write transaction authoritative, preserve ordinary deletion fencing, and avoid exposing Bundle object keys or raw Runtime errors. Verification: focused Rust/OpenAPI and frontend tests plus the Task 7 Runtime browser scenario at desktop and 390px.

- [x] **Task 7b: Make explicit Session ownership release reachable after a committed Bundle.** Preserve the existing generation, current-Bundle, and unreplayable-history fences, but allow the current Runtime owner to call `POST /api/runtime/sessions/{session_id}/release` after the combined Bundle upload has finalized and cleared the saving attempt. Keep queued messages replayable, create the cleanup obligation exactly once, and reject stale owners or Bundles that do not cover the current generation. Verification: focused backend state-machine tests and the public-API Bundle/recovery scenario.

- [x] **Task 8: Cover Automation and scheduler workflows.** Add API/browser scenarios for CRUD, immutable Agent binding, Markdown prompt, manual/webhook/interval/cron behavior, one-time webhook secret, disabled/invalid triggers, scheduler deduplication, Run attribution, history pagination, polling, and Run Console errors. Verification: targeted scenarios including a deterministic short interval and unauthenticated webhook request.

- [x] **Task 9: Cover Integration App and OAuth workflows.** Add API/browser scenarios for app CRUD/secret rotation, Agent delegation, client credentials and authorization code grants, scope validation, userinfo, External Identity/Tenant origin isolation, External Session continuation, attachments, SSE, tool request/result, concurrent message serialization, and immediate permission revocation. Verification: targeted scenarios with app-only and user-level tokens and secret scans.

- [x] **Task 10: Cover Widget and Embed workflows.** Add API/browser scenarios for Embed JWT exchange, per-Agent Widget links, origin/session isolation, session selection, rapid double-submit locking, message/stop, SSE, and parent `ready/resize/session-select/message-submit` events. Verification: targeted browser scenario at desktop/390px plus cross-origin and stale-session negative API checks.

- [x] **Task 11: Close cross-cutting console coverage.** Ensure every navigation page is visited against real services, English and Chinese are exercised, desktop and 390px have no horizontal overflow, loading/error/empty states retain lower-layer evidence, and API Docs/OpenAPI remain public and usable. Verification: route coverage validator and targeted navigation browser scenario with console/network diagnostics.

- [x] **Task 12: Run final gates and reconcile evidence.** Run Node syntax checks, coverage validation, targeted scenario groups, `--type api` without Chromium, one complete default QA run, and one necessary build. Confirm no `agent-hub-qa-*` containers/volumes remain and the existing development environment is still healthy. Update every feature to complete evidence and leave no unchecked plan item. Stop rather than weakening a product contract or hiding a valid failure.

- [x] **Task 12a: Isolate the baseline browser Session scenario in the shared QA environment.** Ensure `02-session-browser` always deletes its temporary Agent after assertions so its completed Session becomes historical and releases Runtime ownership. Re-run only the scenarios invalidated by the leaked online Session, including the dependent Codex rollout pair, without starting a second complete default QA run. Verification: all previously failed scenarios pass together in one fresh selected-scenario environment.

- [x] **Task 12b: Make the fake Codex fixture honor the configured default Model Connection.** When an Agent has default and subagent providers, resolve the top-level `model_provider` and use that provider's Hub connection header instead of the first UUID-sorted provider table. Verification: a focused fixture regression with a lexically earlier non-default provider, followed by the affected shared-environment scenario group.

- [x] **Task 12c: Enforce secret-safe QA artifacts, including Playwright traces.** Centralize every Hub secret prefix and structured credential redaction, sanitize trace archives before retaining them, mask secret-bearing failure screenshots, and recursively inspect plain files and ZIP entries before a run can finish. Verification: focused Node tests prove session cookies and every credential class are removed from trace, JSON, logs, and summaries; sanitize the retained QA artifacts and re-scan them recursively.

- [x] **Task 12d: Restore Runtime-to-Session lock ordering for explicit ownership release.** Lock the authenticated Runtime row before the Session row in `runtime_release_session`, retain all generation and Bundle fences, and add a deterministic PostgreSQL concurrency regression against Runtime force deletion. Verification: focused ignored SQLx test plus the existing Bundle/recovery API scenario.

- [x] **Task 12e: Abort a shared QA run after a worker hard timeout.** Preserve ordinary fail-and-continue behavior, but treat an OS-level worker timeout as an environment-tainting infrastructure failure, mark the remaining selected scenarios as not run, and tear down the one shared Compose environment instead of emitting contaminated evidence. Verification: focused runner test proves no later worker starts after timeout and summaries expose every not-run scenario.

- [x] **Task 12f: Make failure-path scenario cleanup mandatory for Runtime-owning Sessions.** Move Agent and related resource cleanup in `03-provider-error-api`, `07-agent-skill-mcp-api`, and `12-sessions-browser` into outer `finally` blocks that preserve both scenario and cleanup errors. Verification: syntax checks and the three targeted scenarios in one shared fresh environment.

- [x] **Task 12g: Tighten the offline coverage and scenario-structure contract.** Require Rust and Playwright evidence to point to matching test files and declarations, enumerate every legal OpenAPI operation method, require `scenario.json`, `scenario.mjs`, and `README.md` for every scenario, and add the missing Automation README. Verification: focused negative fixtures for false evidence, missing README, and HEAD/OPTIONS/TRACE operation gaps, followed by `--coverage` at 100%.

## Expected Shape

- Approximately 12 API scenarios and 8 Browser scenarios, including the existing three.
- One shared fresh Compose environment per runner invocation.
- No new npm dependency, second Playwright installation, real provider call, or test-only authorization bypass.
- Product code changes only when a new scenario proves an actual contract defect.
