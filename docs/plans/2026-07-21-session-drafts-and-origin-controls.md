# Session Drafts And Origin Controls Delivery Plan

This plan implements the approved Conversation Draft, platform/Agent navigation, and Hub-console External Session read-only contracts in `docs/session-runtime-spec.md`, `docs/v1-spec.md`, and `docs/auth-spec.md`.

## Execution Rule

- Start every implementation task unchecked.
- Mark a task `[x]` only after its code and corresponding verification are complete.
- Add a new unchecked task immediately below the task that exposes a required prerequisite.
- The plan is complete only when every task is checked and the final targeted build passes.

## Tasks

- [x] **Task 1: Align the Session DTO and API contract.** Expose the External Platform display name with Session origins, preserve immutable origin IDs, and update shared types/OpenAPI/client fixtures. Add an explicit conflict response for Hub-console writes targeting External Sessions.

- [x] **Task 2: Enforce Hub-console origin boundaries.** Keep listing, history, events, and external integration continuation available, while rejecting console message creation, steering, and stopping for External Sessions in the backend. Cover both the new-session/run endpoint and existing-session message endpoint without changing Widget or Integration API behavior.

- [x] **Task 3: Implement per-user, per-Agent Conversation Drafts.** Replace the creation dialog with a local browser Draft store keyed by Hub User and Agent. Persist unsent content across refresh/browser close, restore one Draft per Agent, support explicit discard, clear a successful Draft, retain failed first-message content, and clear all of that user's Drafts on explicit logout. Never create or list backend state before the first accepted message.

- [x] **Task 4: Implement platform-first and Agent-aware session navigation.** Order controls as Platform, Agent, Search; default Platform to Hub-native; list All Platforms and named External Platforms; restore the user's last valid Agent selection; filter Sessions by the concrete Agent; and switch back to Hub-native when starting a Draft from an External Platform view. Keep Drafts out of the formal Session list.

- [x] **Task 5: Make External Sessions visibly view-only without losing live history.** Keep ordered messages and readable activity events available, allow relevant event streaming for active External Sessions, and remove composer, steer, and stop controls from the Hub console. Preserve existing Historical/Recovery-failed behavior.

- [x] **Task 6: Add focused API, browser, responsive, and regression coverage.** Test origin names and backend rejection/continuation boundaries, Draft persistence/discard/failure/success/logout semantics, platform and Agent filtering, external read-only rendering, stale-selection races, desktop and 390px layouts, and no console/network regressions.

- [x] **Task 7: Run delivery gates.** Run the focused Rust/TypeScript/Playwright tests and one frontend/backend build required by the diff, inspect the final diff and docs alignment, and stop on any blocking failure rather than weakening the contracts.

- [x] **Task 7a: Preserve Hub/Runtime rolling compatibility for platform display names.** Keep immutable Session Origin limited to its strict identity fields and expose the human-readable External Platform name through a serde-defaulted Session display field, so an old Runtime ignores the new field and a new Runtime accepts an older Hub response. Re-run shared serialization, Runtime construction, OpenAPI, backend API, and browser source-label evidence before the final gate.

- [x] **Task 7b: Resolve final-review Session reachability and contract blockers.** Keep Historical and otherwise non-invocable Session Agents selectable for history without enabling new Drafts, reject External create-run references before Agent invocation authorization can mask the conflict, and align the directly affected Playwright and unattended Session QA scenarios with platform-first Draft navigation.

## Acceptance

- A new Draft is browser-local only until its first accepted message.
- One Hub User can retain at most one Draft per invocable Agent; explicit logout removes all of that user's Drafts in the current browser.
- Platform selection is first and defaults to Hub-native; named external sources and a concrete Agent filter work independently.
- External Sessions remain viewable but cannot be mutated through Hub-console APIs or controls.
- Existing external integration continuation and Hub-native multi-turn behavior remain intact.
