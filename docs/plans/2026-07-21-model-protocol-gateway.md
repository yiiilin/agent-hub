# Model Protocol Gateway Delivery Plan

## Goal

Implement a lightweight, stateless Go protocol gateway embedded with pinned
`github.com/maximhq/bifrost/core v1.7.2`. Agent Hub remains the sole business
control plane and key authority. The gateway receives one Hub-authorized
request at a time, converts OpenAI Responses requests to the selected upstream
protocol, and returns normalized Responses JSON or SSE.

The first delivery supports `openai_responses` as a byte-transparent fast path
and `anthropic_messages` through Bifrost Core. Automatic retry, fallback,
cache, persistence, prompt/output logging, and gateway-side usage accounting
are out of scope and disabled.

## Execution Rule

Progress must be tracked in this file only:

- Start every pending task as unchecked
- Mark each task with `[x]` immediately after implementation and verification complete
- If a task reveals a prerequisite gap, add a new unchecked task directly below it before continuing
- If any task remains unchecked, the project is not complete

## Tasks

- [x] **Task 1: Establish the public model-protocol contract.** Add the upstream protocol enum and its defaults to shared DTOs, PostgreSQL, OpenAPI, model connection read/write APIs, UI copy, and specifications. Existing connections must default to `openai_responses`; the new protocol must be captured in immutable usage/error snapshots. Add focused serialization, migration, CRUD, and authorization tests. Verification: focused Rust tests plus frontend type/build checks.

- [x] **Task 2: Build the standalone Go thin gateway.** Add a pinned Bifrost Core module and an internal HTTP/SSE server with health/readiness endpoints. Define an authenticated Hub-only request envelope that carries the request ID, selected protocol, endpoint, request-scoped credential, original body/query, and safe headers. Implement a no-store/no-retry response path, request-scoped direct key, endpoint override compatibility layer, OpenAI Responses transparent forwarding, Anthropic Messages conversion, streaming, cancellation propagation, and no-secret logging. Write Go unit/integration tests against `httptest` upstreams. Verification: `go test ./...` from `gateway/`.

- [x] **Task 3: Route Hub model calls through the gateway without weakening authorization or accounting.** Add gateway configuration/startup validation and a Hub HTTP client. Preserve the Runtime-to-Hub path, one-query connection authorization, secret redaction, header filtering, original request body/query handling, terminal Responses accounting, timeout behavior, and immutable ledgers. Add focused backend tests proving protocol dispatch, internal auth, request-level key/endpoint isolation, OpenAI transparency, Anthropic normalized terminal usage/error handling, and gateway failure mapping. Verification: targeted backend tests and formatter/clippy for touched Rust code.

- [x] **Task 4: Ship the gateway in development and production deployment.** Add the gateway Dockerfile and Compose service on the internal network only. Inject only the Hub-to-gateway credential and non-secret endpoint configuration into the relevant services; do not give the gateway database, runtime, S3, OAuth, or provider credentials. Ensure the backend waits for gateway readiness while Runtime remains connected directly to Hub. Add deployment documentation and deterministic development fixture behavior. Verification: Compose build and health/readiness checks.

- [x] **Task 5: Complete console and QA coverage.** Expose the upstream protocol choice in Model Connection create/edit/test dialogs, retain secret write-only behavior, and verify protocol visibility in desktop/mobile views. Extend API/browser QA with an Anthropic fixture and gateway-path evidence while keeping existing Responses behavior. Verification: frontend build, focused Playwright/QA scenarios, and console/network checks.

- [x] **Task 6: Synchronize architecture documentation and run final gates.** Update the model connection/proxy specs, add the ADR superseding the transparent-only limitation, update operations and V1 docs, retain the researched gateway evaluation, and record the supported-protocol matrix. Run all affected targeted tests, one Compose-backed QA pass, Rust formatting/clippy, frontend build, Go tests, and one workspace build/test gate. Review the final diff and update this plan only after every required verification succeeds.
