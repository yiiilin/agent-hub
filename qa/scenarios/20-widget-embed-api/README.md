# Widget embed API isolation

Exercises every Widget credential entry point through the real Compose API:
Embed JWT exchange, owner-issued embed sessions, Integration client-credential
exchange, external-user Widget access and renewal, and per-Agent Integration
App Widget sessions. Two Agents and two
Integration Apps on separate External Platform and Authentication Channel
origins prove that issued Widget sessions remain scoped to their Agent and
origin.

The scenario starts one deterministic held Widget Run, reads its authenticated
SSE stream, rejects cross-Session stream and cross-Run stop attempts, then
stops the correct Run. It also verifies trusted-origin CORS preflight behavior,
Embed JWT replay rejection, Agent-delegation invalidation, and Authentication
Channel invalidation without writing opaque credentials to artifacts. The
external-user path verifies the Pi-backed Run, in-place credential rotation,
same-Session continuation, history messages/events/SSE, user and tenant
isolation, and history-off exact-Session recovery.

The scenario restores the channel state, interrupts any active Run, and deletes
its Agents in `finally`. Integration Apps and External Platforms have no delete
API and are discarded with the isolated QA Compose database.
