# Model Connections API

Type: `api`

This scenario exercises Global and Personal Model Connections through the real
Hub, Model Gateway, PostgreSQL, Runtime, fake Codex app-server, and fake
OpenAI Responses/Chat Completions/Anthropic Messages provider. It
checks role and owner boundaries, write-only encrypted API keys, System Default
copy semantics, connection CRUD/test/status/delete behavior, Agent defaults and
subagent overrides, model-unconfigured rejection, caller attribution, successful
and failed usage/error accounting, protocol conversion and immutable protocol
snapshots, millisecond ranges, independent keyset pages, and deletion-safe ledger
snapshots. Connection CRUD also verifies automatic detailed-parameter defaults,
typed create/read/options values, protocol-specific request parameters, protocol
switch reset versus same-protocol preservation, immutable Chat/Anthropic usage
snapshots, explicit updates, and rejection without mutation for mismatched
request parameters.

The original System Default is restored and read back during cleanup. All Agents
and Model Connections use unique names and are deleted or force-deleted after the
scenario. PostgreSQL is used only where no public read API exists: ciphertext and
nonce properties, secret preservation across a keyless update, and secret removal
after deletion. Those queries return only booleans or one-way fingerprints and
never return the provider key.
