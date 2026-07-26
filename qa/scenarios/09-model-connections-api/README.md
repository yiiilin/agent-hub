# Model API Connections, bindings, and accounting

Type: `api`

This scenario runs against an isolated Compose environment with the real Hub,
PostgreSQL, Runtime, stateless Model Gateway, fake Pi RPC process, and fake
Responses/Chat Completions/Anthropic provider.

It proves that a Connection owns only its URL, encrypted key, API Type, scope,
status, and exact multi-model allowlist. It exercises Global/Personal ownership,
write-only randomized credentials, immediate key rotation, flattened model
options, per-model `hi` request/response text and response-time tests, and all
three API types. Legacy
one-connection/one-model fields are rejected.

Agent coverage includes System Default pair copying, explicit connection/model
selection, detailed Agent settings, inherited and overridden subagent settings,
and two immutable Run bindings that share a connection/model but have different
effective settings. Real Runs cross Runtime and Gateway for Responses, Chat,
and Anthropic.

The destructive tail verifies allowlist conflict, explicit force cleanup,
model-unconfigured Run rejection, live credential scrubbing, and retained usage
and error snapshots after Connection deletion. PostgreSQL is queried only for
facts without a public read API: randomized encrypted records, immutable Run
binding rows, and deletion-time credential scrubbing. Queries never return a
plaintext provider key.

The original System Default selection is restored and read back. Scenario-owned
Agents and Connections are removed in reverse order.
