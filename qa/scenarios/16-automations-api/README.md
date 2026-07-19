# Automation API and scheduler lifecycle

Type: `api`

This scenario uses the public Automation APIs against the shared Compose QA
environment. It verifies owner-scoped create, list, and edit behavior; immutable
Agent bindings; manual and anonymous webhook triggers; disabled and invalid
configurations; paginated Run history; and deterministic interval and cron
scheduler execution.

The scenario checks one-time webhook token redaction and Run attribution through
the real Runtime and fake model provider. Cleanup disables created Automations,
archives scenario-owned Agents, and erases the temporary owner.
