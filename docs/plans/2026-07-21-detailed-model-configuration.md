# Detailed Model Configuration Delivery Plan

## Goal

Add detailed, strongly typed Model Connection parameters that map to supported
Codex `config.toml` settings without changing the existing Responses transport
contract. Existing connections and clients must retain their current behavior
when the new `parameters` object is omitted or all values are automatic.

The supported connection defaults are reasoning effort, reasoning summary,
verbosity, context window, automatic compaction threshold, reasoning-summary
capability, service tier, request retries, stream retries, and stream idle
timeout. Agent and Subagent reasoning settings remain overrides. The Hub
and Model Gateway must not parse or rewrite request bodies to inject sampling,
output-limit, or tool-choice parameters.

## Effective Settings

- Reasoning effort precedence is Subagent override, Agent override,
  selected Model Connection default, then Codex model metadata.
- Other model settings come from the Model Connection selected by the main or
  subagent. Unset values are omitted from generated TOML.
- Provider transport settings are emitted into the selected Codex provider
  table. They do not add retry behavior to Hub or Model Gateway.
- Changes apply to the next Turn configuration refresh and do not mutate an
  in-flight Run.

## Execution Rule

Progress must be tracked in this file only:

- Start every pending task as unchecked
- Mark each task with `[x]` immediately after implementation and verification complete
- If a task reveals a prerequisite gap, add a new unchecked task directly below it before continuing
- If any task remains unchecked, the project is not complete

## Tasks

- [x] **Task 1: Define and persist the parameter contract.** Add shared enums and a backward-compatible `ModelConnectionParameters` object, PostgreSQL columns and constraints, CRUD serialization, validation, execution-option loading, and execution fingerprint coverage. Verify focused shared and PostgreSQL model schema/API tests.
- [x] **Task 2: Apply effective settings through native Codex configuration.** Render connection defaults into the main Codex config, render the selected connection into subagent files, preserve Agent/subagent reasoning precedence, and place retry/idle settings in provider tables. Verify focused Runtime rendering and refresh tests, including strict config acceptance by the installed Codex CLI.
- [x] **Task 3: Add detailed console configuration.** Extend Model Connection create/edit dialogs with grouped model, context, and transport controls; retain write-only key handling and current Global/Personal permissions. Add shared English/Chinese copy and responsive styling. Verify the frontend build and focused Playwright coverage at desktop and 390px.
- [x] **Task 4: Synchronize specifications and unattended QA.** Document supported settings, defaults, precedence, protocol behavior, and explicitly unsupported request-body overrides. Extend API and browser QA scenarios for create/read/update/default preservation and effective UI values. Verify the affected QA scenarios against the development Compose environment.
- [x] **Task 5: Run final affected gates and review the diff.** Run formatting, targeted and workspace tests, strict Clippy, Gateway tests if its contract is touched, one frontend build, and one affected browser/QA pass. Confirm no request-body mutation, secret exposure, or unrelated changes before marking the delivery complete.
