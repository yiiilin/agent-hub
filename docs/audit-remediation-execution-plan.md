# Audit Remediation Execution Plan

> Historical plan: its completed direct-model fallback work is superseded by
> `docs/model-connections-spec.md` and ADR-0027. Runtime no longer connects to a
> model provider directly.

> This file tracks the second-pass remediation work derived from `plan.md` and
> `docs/audit-remediation-spec.md`. It is working documentation and is not to
> be committed.

## Execution Rule

Progress must be tracked in this file only:

- Start every pending task as unchecked.
- Mark a task complete only after implementation, automated verification,
  browser acceptance, and both functional and quality review pass.
- Add a new unchecked prerequisite directly below the affected task when one
  is discovered.
- Do not treat the project as complete while any task remains unchecked.

## Batch 1: Agent And Automation Boundaries

- [x] Clear stale Agent detail state during in-app navigation so an old Agent's
  form, runs, and destructive controls cannot operate on the newly selected
  Agent.
  - Acceptance: the route change immediately removes the old detail UI, stale
    requests cannot write it back, and no old payload can PATCH or archive the
    new Agent.
  - Tests: Playwright SPA route transition with a delayed first detail request.
- [x] Add browser coverage for archive versus Automation/scheduler contention.
  - Acceptance: after an archive wins, no enabled Automation or newly created
    run remains for that Agent, and the UI/API return a handled rejection rather
    than a deadlock or a server error.
  - Tests: Playwright concurrent archive/manual trigger and postcondition
    checks.

## Batch 2: Integration Consistency

- [x] Reject OAuth authorize, code exchange, and Integration credentials when
  the target Agent is archived.
  - Acceptance: existing authorization codes and tokens cannot outlive Agent
    archival, including a live SSE connection.
  - Tests: Playwright OAuth/archive/SSE negative-path checks.
- [x] Serialize messages for each Integration session and preserve exact tool
  request/result ownership.
  - Acceptance: concurrent messages do not create sibling active runs;
    non-UUID source tool ids are scoped by run; follow-up context resolves only
    the matching result.
  - Tests: Playwright concurrent API requests and tool-result isolation checks.

## Batch 3: Runtime, Proxy, Skills And Deployment

- [x] Make runtime capability matching enforce effective sandbox and usable
  direct-fallback credentials.
  - Acceptance: a workspace-write Agent never runs on a read-only runtime, and
    direct fallback is advertised and dispatched only when its credential is
    usable.
  - Tests: Rust unit tests plus Compose/browser run coverage.
- [x] Preserve Responses streaming through Hub and runtime proxy layers.
  - Acceptance: stream responses keep their status, content type, and chunked
  body instead of buffering the full upstream response.
  - Tests: Rust local slow-stream proxy test and browser run chain.
- [x] Harden runtime lifecycle and deployment ergonomics.
  - Acceptance: bounded registration backoff, runtime health endpoint and
  Compose healthcheck, non-root app services, and a Docker build context that
  excludes secrets and generated artifacts.
  - Tests: runtime unit tests, Compose inspection, and browser smoke flow.
- [x] Parse runtime-local skill frontmatter structurally and keep MCP error
  handling secret-safe.
  - Acceptance: quoted, multiline, and reordered YAML frontmatter names are
  parsed correctly; malformed files safely fall back; secrets never appear in
  Hub-facing error payloads.
  - Tests: runtime unit tests and MCP browser regression.

## Final Verification

- [x] Run formatting, strict Clippy, all workspace tests, frontend typecheck,
  build, audit, fresh Compose migration/deployment, complete Playwright suite,
  desktop/mobile browser screenshots, and final functional plus quality review.
