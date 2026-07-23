# Separate provider access from Agent model settings

## Context

The first Model Connection shape bound one credential to one model and also
stored Codex behavior and protocol-specific request parameters. Reusing the
same endpoint and key for another provider model required duplicating the
credential, while a small per-Agent parameter change required creating another
connection. A connection ID also stopped being a sufficient Runtime routing
key once two Agents could use the same connection and model with different
effective settings.

Agent Hub V1 has not been released. This decision therefore defines a clean
baseline schema and API. It does not migrate the superseded shape, preserve old
Run snapshots, or accept the old connection-ID proxy header.

## Decision

A **Model API Connection** owns only provider access: display name, immutable
Global or Personal scope, owner where applicable, base URL, encrypted API key,
one API type, a non-empty ordered allowlist of exact model IDs, enabled state,
and timestamps. An allowed ID is not a standalone Model resource. The
canonical API remains `/api/model-connections`, with the final V1 fields only.

Allowlist input is normalized once at the API boundary. Each ID is Unicode
trimmed, must be 1 to 255 Unicode scalar values after trimming, must contain no
control characters, and is compared case-sensitively. Duplicate normalized
IDs are removed while preserving first occurrence. The resulting list must
contain 1 to 256 IDs. Runtime request bodies must use an exact allowed ID.

An **Agent Model Selection** is a connection ID and one model ID from that
connection. A Personal connection is selectable only by an Agent with the same
owner; a Global connection is selectable by every Agent. The **System Default
Model Selection** is one enabled Global connection/model pair copied into new
Agents. It is not a dynamic fallback.

An **Agent Model Settings** object owns Codex behavior and the request settings
for the selected API type. Root Agent fields use explicit automatic values.
Subagent fields may inherit, choose a concrete value, or explicitly return to
automatic behavior. Effective values are resolved field by field in this
order: explicit subagent override, root Agent value, Codex/provider automatic
behavior. A subagent selection override replaces the pair atomically. When its
API type differs from the parent, omitted request settings use that API type's
automatic object instead of inheriting incompatible fields.

Every Run stores immutable, non-secret **Run Model Bindings** for the main
Agent and each distinct explicit subagent configuration. Each binding has its
own UUID and snapshots the selected model ID, API type, effective settings, and
connection identity metadata. Runtime renders controlled Codex providers that
send this binding UUID through the loopback Responses proxy. Hub resolves the
binding within the authenticated active Run, verifies the exact request model,
scope, and live connection enabled state, then loads only the live endpoint and
decrypts the live key. Runtime never receives either secret.

Connection name, allowlist, API type, and Agent setting changes apply when the
next Run binding is created. Base URL and key rotation apply to the next model
request. Disabling or deleting a connection blocks the next request; a request
already handed to Gateway may finish. Run bindings are never rewritten.

Removing an allowed ID that is selected by the System Default, an Agent, or an
explicit subagent returns `409 Conflict`. Changing the API type of any selected
connection also conflicts because its request settings would no longer match.
A force update atomically clears the affected System Default and Agent
selections and disables affected explicit subagent definitions with a visible
reason; it does not silently make them inherit. Force-deleting a connection has
the same reference behavior. Existing
Run bindings and immutable usage/error snapshots are retained, but a binding
whose live connection no longer exists cannot start another provider request.

## Consequences

- One endpoint and key can expose multiple provider model IDs without secret
  duplication.
- Agents sharing one connection/model may still have independent invocation
  behavior because routing uses a Run binding UUID rather than connection ID.
- The Gateway remains stateless and receives one fully resolved request
  envelope; Responses-to-Responses stays byte-transparent, while converted
  protocols receive only their typed effective request settings.
- Usage and error rows snapshot connection name, scope, API type, model ID, and
  effective settings, so deleting live configuration cannot alter history.
- Development and test databases must be rebuilt for this V1 cutover. There is
  intentionally no legacy schema, API alias, backfill, or in-flight Run path.

## Rejected alternatives

- A standalone Model table adds ownership, lifecycle, and join semantics that
  are unnecessary for an exact provider allowlist.
- Keeping invocation defaults on connections recreates connection records for
  every tuning variation and prevents clean Agent/subagent inheritance.
- Routing by connection ID cannot distinguish two effective settings that use
  the same connection and model.
- Sending endpoint or key to Runtime weakens the existing Hub-owned credential
  boundary and is not required for Codex's Responses-only loopback contract.
