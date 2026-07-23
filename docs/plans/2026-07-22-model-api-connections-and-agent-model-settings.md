# Model API Connections and Agent Model Settings

## Goal

Replace the current one-connection/one-model design with a lightweight
Sub2API-style configuration. A Model API Connection owns provider access and a
model allowlist; an Agent selects one allowed model and owns the settings that
control how Codex invokes it.

The change must preserve encrypted credentials, Global/Personal scope,
historical usage and error semantics in the final schema, and the
Runtime-facing Responses contract. Agent Hub V1 is not yet released, so this
cutover targets a freshly rebuilt database and deliberately provides no legacy
schema, API, or in-flight Run compatibility.

## Confirmed Contract

- A Model API Connection contains a display name, scope, base URL, write-only
  API key, one API type, a non-empty exact allowlist of model IDs, and enabled
  state.
- There is no separately managed Model resource and no duplicated credential
  per model. One connection can expose multiple model IDs.
- An Agent Model Selection is one permitted connection ID plus one model ID
  from that connection's allowlist.
- Agent Model Settings contain invocation behavior. Codex subagents inherit the
  Agent selection and settings unless they explicitly override them.
- A System Default Model Selection is a Global connection/model pair copied
  into newly created Agents.
- A Run stores immutable effective binding snapshots. Later connection, model
  restriction, Agent, or subagent changes apply to the next Turn and do not
  rewrite historical Runs or ledgers.
- Provider secrets remain in Hub. Runtime still calls its loopback Responses
  proxy and Model Gateway remains stateless.

## Settings Ownership

The Model API Connection no longer owns model invocation defaults. The Agent
owns the current detailed settings:

- reasoning effort, reasoning summary, verbosity;
- context window, automatic compaction threshold, and reasoning-summary hint;
- service tier, request/stream retry limits, and stream idle timeout;
- API-type-specific request settings such as `temperature`, `top_p`, and the
  applicable output-token limit.

Unset Agent values preserve Codex or provider automatic behavior. A subagent
may override individual fields. Effective precedence is subagent override,
Agent value, then Codex/provider automatic behavior.

## Runtime Binding Boundary

Connection ID alone is no longer sufficient for routing: two Agents or
subagents may use the same connection and model with different settings. Each
Run therefore creates non-secret binding snapshots for the main Agent and each
explicit subagent override. Runtime sends a run-scoped binding ID to Hub; Hub
resolves it to the selected connection, model ID, API type, and effective
request settings before loading the live endpoint and decrypting the key.

Usage remains attributed to the Hub Agent and initiating subject; the binding
ID is routing state and does not add main/subagent attribution to usage views.

## V1 Cutover

- Define the final connection allowlist, Agent/subagent selection fields,
  Agent and subagent settings, System Default model ID, and Run binding
  snapshots directly in the baseline schema.
- Rebuild development and test databases. Do not backfill the superseded
  one-connection/one-model fields or preserve old migration versions.
- Runtime and Hub accept only run-scoped binding IDs. There is no legacy
  connection-ID proxy header or old Run compatibility path.
- The final schema keeps immutable usage/error snapshots. Renaming or deleting
  live connections must not change historical totals produced after cutover.
- `/api/model-connections` remains the canonical V1 connection API, but its old
  one-model fields are removed rather than aliased.

Removing an allowed model that is still selected by an Agent, subagent, or the
System Default returns a conflict. An explicit force operation may clear only
the affected selections; their history remains readable and affected Agents
become model-unconfigured until a valid pair is selected.

## Non-Goals

- No standalone model catalog, price catalog, model aliasing, wildcard model
  rules, or automatic dependency on `/v1/models`.
- No Gateway provider registry, durable Gateway key storage, fallback, or
  retry engine.
- No change to Runtime's Responses-only wire contract and no main/subagent
  usage split.

## Stop And Rollback Conditions

- Stop if a clean database cannot enforce one exact connection/model pair for
  every executable Agent, subagent override, System Default, and Run binding.
- Stop if the new binding ID cannot distinguish two effective settings that
  share a connection and model.
- Stop if any read DTO, log, test artifact, Runtime file, or Gateway error
  exposes a provider key.
- Before deployment, rollback is the previous application image with a freshly
  initialized V1 development/test database. No production-data migration or
  automated reset of an existing production database is in scope.

## Execution Rule

Progress must be tracked in this file only:

- Start every pending task as unchecked
- Mark each task with `[x]` immediately after implementation and verification complete
- If a task reveals a prerequisite gap, add a new unchecked task directly below it before continuing
- If any task remains unchecked, the project is not complete

## Tasks

- [x] **Task 1: Finalize the contract and schema invariants.** Add an ADR for the provider-connection/model-selection split; update the model connection, proxy, driver and V1 contracts and document the intended OpenAPI shapes; define exact allowlist normalization, API-type-specific Agent settings, effective-setting precedence, force-removal behavior, clean-database cutover, and binding-only Runtime routing. Verify documentation links, schema examples, and `git diff --check`.

- [x] **Task 2: Introduce the persisted and shared types.** Replace the baseline schema and typed shared DTOs with Model API Connections, Agent/subagent model selections and settings, System Default selection, and Run binding snapshots. Remove superseded connection-level model/settings fields, rebuild the test database, and add serialization plus PostgreSQL constraint tests.

- [x] **Task 3: Replace connection CRUD and selection authorization.** Implement scoped Model API Connection CRUD with exact model allowlists, encrypted key rotation, per-model connection testing, status/delete/force semantics, flattened connection/model options, and System Default pair management. Validate Global/Personal and Administrator boundaries, referenced-model removal, referenced API-Type changes, model-unconfigured behavior, and key non-disclosure with focused backend tests and OpenAPI assertions.

- [x] **Task 4: Route model calls through immutable Run bindings.** Generate main/subagent binding snapshots, render one controlled Codex provider per effective binding, send the run-scoped binding ID through Runtime's loopback proxy, and have Hub validate binding, model ID, live connection status, API type, and scope before calling Gateway. Preserve exact protocol conversion, request settings, usage/error accounting, cancellation, and next-Turn refresh; reject every connection-ID-only request. Verify Runtime rendering/app-server tests, backend proxy tests, and Gateway tests.

- [x] **Task 5: Move invocation settings into Agent workflows.** Replace the model page's detailed parameter groups with the minimal connection form and model allowlist editor. Update Agent and subagent forms to select connection plus model ID and configure inheritable detailed settings, showing the effective value and its source. Update System Default selection, i18n, responsive styling, and API client types; verify frontend build and focused Playwright coverage at desktop and 390px with console/network checks.

- [x] **Task 6: Preserve history and unattended QA.** Update usage/error snapshots and grouping to retain connection name, API type, model ID, and effective request settings after live deletion. Extend API/browser QA for one connection with multiple models, two bindings sharing a connection/model but using different settings, key rotation, allowlist conflict/force removal, Agent/subagent inheritance, historical retention, and all three API types. Verify the affected QA scenarios against an isolated clean Compose project and scan artifacts for secrets.

- [x] **Task 7: Cut over the development environment and run affected gates.** Rebuild the development environment from an empty database, recreate the Sub2API-style fixture as one connection with multiple allowed models, then run formatting, targeted Rust tests, strict Clippy for affected crates, Gateway tests/build, frontend build, focused browser QA, and one workspace build. Verify the final schema and binding-only route contain no legacy fields or aliases, then perform one final diff review.
