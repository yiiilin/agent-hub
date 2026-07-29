# Browser SDK And Client Tools Delivery Plan

This plan implements the approved Client Access Credential, per-Run Client Tool, browser SDK, Widget reuse, and Integration App configuration contracts in `docs/integration-spec.md` and ADR-0031.

## Execution Rule

- Start every implementation task unchecked.
- Mark a task `[x]` only after its code and corresponding focused verification are complete.
- Add a new unchecked task immediately below the task that exposes a required prerequisite.
- Do not expand into unrelated Widget, Session, Agent, OAuth, or frontend redesign work.
- The plan is complete only when every task is checked, the targeted browser evidence is current, and the final build passes.

## Tasks

- [x] **Task 1: Align persisted contracts and shared DTOs.** Extend existing Integration App, credential, Run, and tool-request records without adding a grant table; add validated Client Tool definitions, tab-scoped Client Instance binding, immutable Run Tool Snapshot, claim/batch states, structured results, five-minute deadlines, and OpenAPI/shared/client types. Preserve existing Session and historical tool records.

- [x] **Task 2: Implement generalized Client Access authorization.** Add canonical `/api/client/*` access and renewal handlers for trusted backends and anonymous browsers, atomic same-instance token rotation, multi-Session scope checks, optional authenticated Origin enforcement, mandatory anonymous Origin enforcement, live App/User/Agent/delegation checks, and compatibility aliases for current Widget routes.

- [x] **Task 3: Implement Client Session and event APIs.** Support delayed first-message Session creation, one credential across matching Sessions, history on/off, exact anonymous current-Session recovery, stable message keys, active-Turn steering, stop, message pagination, and resumable typed Session SSE without allowing cross-origin or cross-identity access.

- [x] **Task 4: Implement the Run-bound Client Tool state machine.** Freeze the credential Grant on every Run, bind the Run Tool Executor, validate the Agent `integration` gate, atomically claim calls, reject cross-instance execution, accept structured and size-limited results idempotently, serialize batch completion into one continuation, and fail unknown/interrupted/timed-out batches without replay.

- [x] **Task 5: Adapt Runtime and Pi continuation.** Translate protocol-neutral Client Tool definitions to collision-free internal Pi names, map events back to external names, carry full ordered result batches into the same Native Session, preserve external user and Run Snapshot context across continuation, and cover normal errors versus unknown outcomes.

- [x] **Task 6: Build `@agent-hub/client`.** Add the framework-neutral ESM package with declarations, per-tab Client Instance initialization, application `authorize` callback, anonymous access, in-memory token renewal and one-shot fallback, Session/draft APIs, message idempotency, SSE cursor reconnect, IndexedDB/custom journal, serial handler dispatch, claim/result idempotency, `reauthorize()`, cleanup, and `npm pack` verification.

- [x] **Task 7: Reuse the SDK in Widget and expose App configuration.** Replace Widget-owned client state machinery with the SDK core, add Integration App Client Tool list/add/edit/delete forms, enforce anonymous configuration rules, and render ordered collapsible Client Tool technical events with status and elapsed time in the platform and Widget at desktop and 390px.

- [x] **Task 8: Publish integration documentation and QA evidence.** Document trusted backend HTTP authorization, vanilla TypeScript, React, anonymous access, Origin/HTTP risks, tool handlers, side-effect idempotency, errors, recovery, and package installation. Update OpenAPI, V1/Pi/QA specs and feature mappings; add Rust, Runtime, SDK, API, and Browser scenarios for every acceptance branch named in the integration spec.

- [x] **Task 9: Run delivery gates and final review.** Run focused tests after each task, then SDK build/pack, affected Rust tests, frontend build, one workspace/backend build, and targeted Playwright scenarios. Inspect browser console/network and desktop/390px views, run one final read-only review, fix only valid blocking findings, and leave non-blocking suggestions in the delivery report.

## Acceptance

- Browser credentials are short-lived, restart-safe, memory-only, scoped to one App/User/Agent/tab, and usable across matching Sessions without exposing server authority.
- Client Tool definitions come only from a trusted backend or administrator-managed anonymous App configuration and are frozen per Run.
- One model batch executes serially at most once, cannot move to another tab, and produces one continuation only after safe terminal results.
- Authenticated and anonymous Widget flows, direct SDK flows, history rules, multi-tab isolation, SSE recovery, and two-turn conversations have current automated evidence.
- The Widget and external clients use the same SDK core; `@agent-hub/client` is buildable and packable, and the documented examples match the shipped API.
