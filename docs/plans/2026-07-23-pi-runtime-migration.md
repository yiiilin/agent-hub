# Pi Runtime Migration Plan

## Goal

Replace the Runtime's Codex app-server execution process with a pinned Pi
standalone RPC process. Keep the Hub control-plane contracts, the local
Responses model proxy, Hub-held provider secrets, model usage/error accounting,
Session ownership, Workspace isolation, persisted skills, message ordering,
steering, interruption, and idle Bundle lifecycle.

## Confirmed Scope

- Pi is introduced as the pinned `third_party/pi` git submodule at `v0.81.1`.
- Linux x64 builds use Bun `1.3.14` with `bun-linux-x64-baseline`; this is
  required for the Runtime's non-AVX host compatibility.
- Runtime uses Pi RPC over stdin/stdout. It does not embed Node.js or Bun at
  runtime; the complete Pi release directory is copied into the Runtime image.
- Hub remains the model gateway. Pi always calls the Runtime-local
  OpenAI Responses endpoint; Hub keeps protocol conversion, provider keys,
  token accounting, error records, and per-Run binding authentication.
- Managed Skills and Agent instructions continue to be materialized for each
  Session. MCP and Codex subagents are deliberately disabled for Pi in this
  iteration.
- Existing public DTO, database, API, and console field names remain stable.
  Compatibility-named `codex_*` fields temporarily report the Pi artifact
  version and Pi native session id rather than a Codex executable/thread.
- The existing Session Bundle wire format is retained for this iteration. The
  compatibility `codex-thread/` archive subtree contains only Pi's saved
  session data; generated model, Skill, and secret configuration remains out
  of Bundles and is regenerated after restore.
- No direct-provider migration, model-gateway removal, MCP implementation,
  subagent implementation, or unrelated UI redesign is included.

## Execution Model

```text
Hub Session -- one persistent Runtime supervisor -- one Pi RPC process
       |                                         |
       |                                         +-- workspace/
       |                                         +-- isolated Pi agent home
       |                                         |     .pi/agent/models.json
       |                                         |     .pi/agent/AGENTS.md
       |                                         |     .pi/agent/skills/
       |                                         +-- Pi JSONL session files
       |
       +-- many ordered Hub messages / Hub Runs / one active Pi turn
```

Pi is started with an isolated `HOME`, `--mode rpc`, a per-Session
`--session-dir`, a Hub-generated model provider, and only the tool set allowed
by the existing sandbox mode. A Pi `turn_start` is the point at which the
local model proxy becomes eligible to forward the active Run's request.

## Acceptance Claims

1. A clean source checkout builds a pinned Pi baseline artifact and the
   standalone binary reports the pinned version without a Node/Bun runtime.
2. A Hub Session starts one Pi RPC process, streams text/thinking/tool events,
   reaches a terminal Run status, and forwards its model request through the
   existing binding-authenticated local proxy.
3. A second Hub message reuses the same Pi session; a steering message reaches
   the active Pi turn exactly once; an interrupt stops the active turn without
   erasing already executed work.
4. A Bundle contains the Workspace and Pi recovery state but no model key,
   model proxy token, or generated Skill/configuration secret; restore resumes
   the same Hub Session with a newly materialized configuration.
5. The existing Hub-side Responses gateway continues to own provider secret
   use, usage records, and error records for Pi-originated model requests.

## Execution Rule

Progress must be tracked in this file only:

- Start every pending task as unchecked
- Mark each task with `[x]` immediately after implementation and verification complete
- If a task reveals a prerequisite gap, add a new unchecked task directly below it before continuing
- If any task remains unchecked, the project is not complete

## Tasks

- [x] **Task 1: Freeze the Pi driver contract and test fixtures.** Added `docs/pi-driver-spec.md`, Session/operations compatibility notes, and `deploy/fake-pi-rpc.sh`. The fixture implements strict Pi JSONL `get_state`, model/thinking setup, prompt, steer, abort, state persistence and streaming events while rejecting Codex app-server requests. Verification (2026-07-23): `cargo test -p agent-hub-runtime fake_pi_fixture_uses_pi_jsonl_not_codex_app_server` passed; `sh -n deploy/fake-pi-rpc.sh` and `git diff --check` passed.

- [x] **Task 2: Add the pinned Pi source and reproducible standalone builder.** Added gitlink `third_party/pi` at `v0.81.1` / `20be4b18d4c57487f8993d2762bace129f0cf7c6`, a 38-file versioned model-data snapshot, and `scripts/build-pi-standalone.sh`. The script verifies the gitlink, snapshot tree SHA-256, Bun `1.3.14` archive SHA-256, baseline target, full Pi runtime resource manifest, and final version. Verification (2026-07-23): fresh `npm ci --ignore-scripts` + snapshot `build:offline` + baseline compile passed; the default verified Bun-download path also passed with `--skip-install`; the resulting 112 MB release reported `0.81.1` and served RPC `get_state` with `PATH=/usr/bin:/bin`, where neither Node nor Bun was available.

- [x] **Task 3: Materialize isolated Pi configuration without changing Hub contracts.** Added per-Session `.pi/agent` materialization for instructions, managed/local Skills, a binding-scoped local Responses provider, Pi thinking/output metadata, private marker-protected files, stale Skill cleanup, and refresh-time model preservation while excluding MCP and subagents. Verification (2026-07-23): both Pi-specific materialization tests passed; all six `materialization`-filtered Runtime regressions, the MCP secret isolation test, Hub-over-local Skill test, fake Pi protocol fixture test, Rust formatting, and `git diff --check` passed. Existing Bundle source selection excludes `.pi/agent`; Task 6 replaces the Codex recovery whitelist with an explicit Pi JSONL whitelist.

- [x] **Task 4: Implement the Pi persistent-process adapter and event translator.** Added the persistent Pi RPC process, strict request/response correlation, isolated launch and process-group cancellation, event translation and duplicate suppression, and routed production Session execution through Pi. Verification (2026-07-23): all five `persistent_pi_rpc_process` tests passed for completed/failed/interrupted terminal states, normal streaming, malformed JSON, timeout, duplicate events, and child reaping; `cargo check -p agent-hub-runtime --tests` and `cargo fmt --all --check` passed.

- [x] **Task 5: Preserve Session supervision, steering, interruption, and proxy fencing.** Pi now runs through the existing Session supervisor and manager state machine, retains its native JSONL id across Runs/cold recovery, acknowledges the local proxy only after persisted `turn_started`, and sends active input as exact-once `steer` or `abort`. Verification (2026-07-23): 15 Pi-focused Runtime tests passed; Pi supervisor tests covered same-PID two-Run continuity, exact-once steer, interrupt-without-rollback followed by a completed Run, and discovered-JSONL cold restart. The converted managed-session integration test passed with Pi and verified delayed turn acknowledgement plus atomic Run-token/binding switch on one proxy listener. Existing manager concurrency and Hub steer-retry idempotency tests also passed.

- [x] **Task 5a: Make next-Turn model changes reloadable without restarting Pi.** Added a checksum-pinned, single-command Pi build patch for `reload_models`; the builder applies it to a temporary archive of the pinned commit and verifies the compiled RPC JS before producing the baseline binary. Runtime invokes it offline before every `set_model`, preserving the original Responses body and persistent process. Verification (2026-07-23): the fake-process red/green test switched from `gpt-main` to `gpt-second` on one PID; the rebuilt real standalone completed two Runs on one Pi Session and the HTTP oracle observed each exact model ID and Run Model Binding; the full `--skip-install` baseline build passed.

- [x] **Task 6: Keep Bundle and idle lifecycle semantics with Pi recovery data.** Bundle creation now identifies exactly one Pi recovery JSONL by its native Session header instead of its filename, and restore accepts only the compatibility `codex-thread/sessions/<file>.jsonl` tree before validating the restored header. Runtime checkpoint/restore tests prove that model/auth/settings/Skills/extensions/cache state and other Session files are excluded, and that the restored native Session completes another Pi Turn. Verification (2026-07-23): all eight `session_bundle_` tests, 19 `checkpoint` tests, 15 `idle_` tests, six `drain` tests, the version-checkpoint test, `cargo fmt --all --check`, and `git diff --check` passed.

- [x] **Task 7: Package Pi into Runtime images and switch development/production configuration.** The Runtime image now builds the pinned Pi release in a Node build stage and copies only the standalone release into the non-root final image. Production and development Compose no longer mount or default to an external Codex executable. The compatibility rollout DTO remains accepted, but the normal Pi `app-server` compatibility driver clears candidates without downloading artifacts or scheduling a version checkpoint. Verification (2026-07-23): `docker buildx build --check -f deploy/runtime.Dockerfile .`, both Compose configs (production with required placeholder variables), a built image running as UID/GID 10001 with Pi `0.81.1`, `curl`/`jq` present and Node/npm/Bun absent, plus the targeted inert-rollout and legacy rollout regression tests passed. The Worker also built the development Runtime image; full Compose conversation smoke is completed in Task 8.

- [x] **Task 8: Update focused documentation and QA scenarios.** Updated operational documentation for Pi version pinning, artifact rebuild, model-data freshness, runtime resource layout, Session recovery, and rollback to the previous image. Added/adjusted API/browser QA only for the directly affected Session behavior and verified desktop plus 390px conversation state does not regress. Verification (2026-07-23): `cargo fmt --all --check`, `cargo clippy -p agent-hub-runtime --all-targets --all-features -- -D warnings`, `cargo test -p agent-hub-runtime` (`179 passed, 1 ignored`), frontend `npm run build`, targeted QA artifact `qa/artifacts/2026-07-23T08-13-03-033Z/summary.json` (`02-session-browser`, `11-session-run-api`, `12-sessions-browser`: 3 passed), and a real Pi Compose browser smoke with 1280px/390px overflow both `0` and no unexpected browser diagnostics. The separate historical `16-pi-session-recovery-api` diagnostic remains outside this focused Task 8 run and failed on its sentinel assertion; existing Runtime recovery unit tests passed in the targeted Rust suite.

- [x] **Task 8a: Reinstate the Pi security boundary before final verification.** Production no longer materializes legacy Codex configuration, reports MCP as disabled, gives Pi dedicated Home/config/session/temp directories, and installs a fail-closed Linux Landlock ABI 2 allowlist in the child before `exec`. The only procfs exception is the forked child's own `/proc/self/maps`, which the Bun standalone requires during startup; sibling Session paths and legacy Codex files remain denied. Verification (2026-07-23): all three Landlock probes passed, including denied legacy/sibling reads, allowed own Pi-state writes and Workspace rename, and child-only proc maps access; `cargo test -p agent-hub-runtime` passed (`182 passed, 1 ignored`), strict Runtime Clippy, Rust formatting, `git diff --check`, and frontend build passed. A final Runtime image built as UID/GID 10001 with Pi `0.81.1` and no Node/npm/Bun, while a hostile host `RUNTIME_CODEX_VERSION=0.144.6` could not override the image or Hub-reported `0.81.1`. Real Run `9f6aae48-8056-4a44-9adb-950082cd9a27` completed through the Responses gateway. QA artifacts `qa/artifacts/2026-07-23T13-44-41-936Z/summary.json` (`02`, `11`, `12`: 3 passed) and `qa/artifacts/2026-07-23T13-40-19-367Z/summary.json` (`16`: 1 passed) prove browser/API conversation behavior plus default-seccomp idle Bundle recovery with the same native Pi Session id, regenerated configuration, and no persisted sentinel.

- [x] **Task 9: Run one final read-only blocking review and close the migration.** Reused the existing independent `reviewer` thread for the sole final read-only gate. It found no verified blocking finding and no non-blocking rework item after checking the plan, final state, compatibility fields, Bundle boundary, image pin, gitlink pin, and supplied Run/QA/isolation evidence. Its recorded residual risk is that the interrupted review did not independently complete a line-by-line audit of the full large Runtime/Pi builder/model-data/QA diff and relied on the fresh verification evidence; this is not an established defect and does not reopen implementation. All preceding checklist items are checked.

## Stop Conditions

- Stop instead of silently weakening secret isolation, per-Session filesystem
  isolation, model-proxy Run/binding authentication, or user-visible
  interruption semantics.
- Do not use a real provider secret in fixture output, source code, snapshots,
  or Bundle artifacts.
- If Pi cannot represent an existing setting without a defined mapping, retain
  the setting in the Hub contract and report the gap rather than inventing a
  provider request.
