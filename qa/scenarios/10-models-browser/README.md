# Model API Connections, Agent settings, references, and retained usage

Type: `browser`

This scenario uses the real console, Hub backend, Runtime, stateless Model
Gateway, PostgreSQL, and fake provider. Shared browser support captures console,
page, request, same-origin HTTP, trace, screenshot, and artifact-safety failures.

A member creates and edits one Personal Model API Connection with three allowed
model IDs, sends the default `hi` test message to an explicit successful and
failing model, checks the returned text and response time, and verifies that the
API key is write-only. Through the Agent console, the member selects a complete
connection/model pair, enters detailed Agent model settings, and creates both an
inheriting subagent and a subagent with an explicit pair and settings override.
Enum values, API Type, and setting values are checked as their raw tokens.

The member starts a real Run through Runtime and Gateway. After the Agent and
Personal connection are deleted, the Usage page still renders the snapshotted
connection name and original model IDs. The same browser session checks the real
ledger response for the retained API Type and typed request settings because the
current Usage table does not render those two fields.

An Administrator creates a multi-model Global connection and sets System
Default to an explicit connection/model pair. A newly created Agent copies that
pair. Removing its referenced model through an ordinary allowlist update returns
`409`; an explicit `force=true` update clears the root selection and System
Default while preserving an unaffected subagent pair. After the Agent is rebound
to the remaining model, ordinary delete conflicts and Force Delete leaves the
Agent visibly model-unconfigured and disables the affected subagent definition.

The scenario checks document overflow at 1280x800 and 390x844 across Models,
Usage, Agent settings, and destructive dialogs. API key entry occurs while trace
capture is paused. Scenario-owned Agents and connections are removed, the
temporary Administrator role is reverted, and the original System Default pair
is restored and read back even after a scenario failure.
