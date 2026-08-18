<div align="center">

# Agent Hub

**A self-hosted workspace where agents work in isolated runtimes and report back to you.**

Agent Hub pairs a Rust control plane with disposable Pi-driven runtimes: every session gets
its own sandboxed workspace, its own model bindings, and a full audit trail of reasoning,
tool calls, and token usage. Chat with agents, embed them in your product, or let them run
on schedule — all on infrastructure you control.

[![Release](https://img.shields.io/github/v/release/yiiilin/agent-hub?style=flat)](https://github.com/yiiilin/agent-hub/releases)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[Getting started](#get-started) · [Self-hosting](#self-hosting) · [Architecture](#architecture) · [Docs](docs/) · [License](#license)

</div>

---

## What is Agent Hub?

Most agent platforms either lock you into a hosted service or make you babysit a terminal.
Agent Hub is the middle path: run the whole control plane yourself, let agents execute in
isolated per-session runtimes, and keep the conversation, the workspace, and the model bill
connected to the same session.

- **Hub** — the control plane: sessions, agents, skills, model connections, usage accounting,
  authentication, and the management console.
- **Runtime** — an independent execution node powered by [Pi](https://github.com/earendil-works/pi).
  Each session gets its own sandboxed workspace (`/workspace`), temp dir (`/tmp`), and engine
  state; a crashed runtime can be reclaimed and its workspace salvaged back to the Hub.
- **Model proxy** — Hub authorizes every run-scoped request, then forwards it straight to the
  configured upstream (Responses / Chat Completions / Anthropic Messages), rewriting auth and
  filtering security headers while passing the body through verbatim — no protocol translation.
  Per-connection API keys, optional vision models, and per-protocol usage/error history.

## Talk to the agents.

*One session, one workspace, one bill.*

- **Chat like ChatGPT** — markdown rendering, activity steps (thinking, tools, commands),
  auto-generated session titles, and attachment upload with inline image preview.
- **Model connections** — global and personal connections with per-model settings, optional
  `vision_model_id` for image analysis via the built-in `vision_analyze` tool, and detailed
  usage/error history per agent and per user.
- **Skills & secrets** — package skills with executables, declare user secrets, and grant them
  per session; the runtime injects them into the sandbox only when authorized.

## Embed them in your product.

*Authentication or anonymous, with your own client tools.*

- **Widget** — drop an iframe into any site; support authentication via Client Access
  Credentials or fully anonymous sessions, with optional conversation history.
- **Integration apps** — OAuth-style apps with scoped identities, tool grants, and
  at-most-once client tool calls over SSE.
- **SDK** — a TypeScript SDK for embedding, sending messages with attachments, and handling
  tool requests.

## Run them on your own schedule.

*Automations, runtimes, and recovery that don't need you.*

- **Automations** — interval or cron-triggered runs with archived history.
- **Runtime nodes** — enroll, bind agents, drain, delete, or force-delete; a crashed runtime's
  sessions are reclaimed automatically, and surviving workspaces are re-uploaded as bundles.
- **Upload retries & idempotency** — runtime uploads retry with backoff, the Hub acknowledges
  idempotently, and heartbeat reconciliation recovers stale ownership.

## Get started

Requires Docker (and optionally PostgreSQL/MinIO if you skip Compose).

```bash
git clone https://github.com/yiiilin/agent-hub.git
cd agent-hub
docker compose up -d
```

The console is served by the Hub container (default port `8080`, configurable via
`FRONTEND_PORT` in `compose.yml`). Create the first account, then add a model connection and
an agent.

For local development:

```bash
docker compose -p agent-hub-dev -f compose.dev.yml up -d --build
# console: http://localhost:15173 (override with FRONTEND_PORT)
```

## Self-hosting

- `compose.yml` — production stack: Hub, Runtime (optional profile), PostgreSQL, MinIO.
- `compose.maintenance.yml` — optional maintenance override that binds the built-in
  `agent-hub-maintenance` skill to a private agent.
- Images are published to GHCR (`ghcr.io/yiiilin/agent-hub`, `agent-hub-runtime`); the CLI
  (`agent-hub-cli`) is attached to each GitHub release.

Runtime nodes do not need to run on the same machine: enroll a node anywhere and it pulls work
from the Hub over the network.

## Architecture

```
┌────────────────────────────┐
│       Runtime (Rust)       │
│  Pi standalone per session │
│  sandboxed workspace       │
└──────────┬─────────────────┘
           │ model requests / heartbeat /
           │ claim / events
┌──────────▼─────────────────┐      ┌──────────────────────┐
│        Hub (Rust)          │ ───► │   Model providers    │
│  HTTP API · auth · sessions│      │  Responses / Chat    │
│  React console · model     │      │  Completions /       │
│  proxy · usage accounting  │      │  Anthropic Messages  │
└────────────────────────────┘      └──────────────────────┘
```

The Hub never calls runtimes directly: runtimes poll for work, stream events, upload session
bundles, and acknowledge state changes. The Hub is the single source of truth for sessions;
runtimes are disposable executors. Runtime model requests go through the Hub model proxy: the
Hub authorizes the active Run binding, decrypts the provider credential, and forwards the
request straight to the upstream endpoint — rewriting auth and filtering security headers
while passing the body through verbatim. No protocol translation happens anywhere.

## Building & testing

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cd frontend && npm ci && npm run build
./qa/run-all.sh          # unattended API + browser scenarios
```

Integration tests that need PostgreSQL require `DATABASE_URL` with `CREATE DATABASE` rights;
S3-backed bundle tests additionally need `BUNDLE_S3_INTEGRATION_*` variables.

## Docs

- [v1 specification](docs/v1-spec.md) — scope and feature overview
- [Authentication](docs/auth-spec.md) — passwords, LDAP, API keys, embed tokens
- [Automations](docs/automation-spec.md) — interval and cron scheduling
- [Third-party integration guide](docs/) — embedding, SDK, and Client Tools

## License

[MIT](LICENSE) © 2026 yiiilin
