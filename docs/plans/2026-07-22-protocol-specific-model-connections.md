# Protocol-Specific Model Connections

## Confirmed Contract

- A Model Connection has exactly one upstream protocol: `openai_responses`, `openai_chat_completions`, or `anthropic_messages`.
- `parameters` remains the Codex-native runtime configuration. `request_parameters` is a separate protocol-specific request configuration.
- `openai_responses` keeps the existing request and response byte-transparent path. It accepts no configured sampling or output-limit override.
- `openai_chat_completions` may configure `temperature`, `top_p`, and `max_completion_tokens`; `anthropic_messages` may configure `temperature`, `top_p`, and `max_tokens`.
- An exact-protocol request is passed through. A Responses request converted to Chat Completions or Anthropic Messages is rejected before forwarding when its fields, tools, or history cannot be represented without loss.
- The Gateway returns Responses JSON/SSE to Codex for every supported upstream protocol.

## Execution Rule

Progress must be tracked in this file only:

- Start every pending task as unchecked
- Mark each task with `[x]` immediately after implementation and verification complete
- If a task reveals a prerequisite gap, add a new unchecked task directly below it before continuing
- If any task remains unchecked, the project is not complete

## Tasks

- [x] Add typed protocol-specific request parameters to shared DTOs, migration `0004`, CRUD validation, OpenAPI, run snapshots, usage/error snapshots, and Hub-to-Gateway envelope; cover serialization and schema behavior with Rust tests.
- [x] Extend Gateway dispatch for `openai_chat_completions`; merge Anthropic request parameters; preserve the Responses transparent path; reject non-representable Responses features before conversion; cover JSON/SSE, headers, parameter merging, and rejection behavior with Go tests.
- [x] Render protocol-specific model request settings separately from Codex runtime settings in the model connection dialog; reset the request settings when the protocol changes; use raw protocol enum values in UI; cover with Playwright tests.
- [x] Update model connection/proxy/driver specifications and QA scenarios, run targeted Rust/Go/frontend/QA checks plus build, and record fresh evidence before marking this item complete.
