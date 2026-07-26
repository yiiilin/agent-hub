# Automation browser workflows

Uses the real console, backend, PostgreSQL, scheduler, Runtime, fake Pi RPC, and
fake Responses provider. The scenario creates and edits Automations through the
UI, exercises Markdown source and rich-text modes, proves the Agent binding is
immutable, and validates the shared row structure for manual, webhook,
interval, and cron triggers.

Manual and unauthenticated webhook requests create attributed Runs through the
public API chain. A deterministic `2s` interval and a `* * * * *` cron produce
scheduled history before both Automations are disabled. Twenty-one manual Runs
exercise active-history polling and pagination; the oldest deterministic fake
provider failure opens the exact Run Console error from persisted events.

The one-time webhook token is copied while tracing is paused. The dialog is
closed and its secret DOM is removed before tracing resumes. Desktop and
390x844 checks require no horizontal overflow, English and Chinese surfaces are
exercised, and shared browser diagnostics reject unexpected console, page,
request, and HTTP errors. Cleanup disables every created Automation and archives
the scenario-owned Agent through public APIs.
